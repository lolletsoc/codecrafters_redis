use dashmap::DashMap;
use deku::bitvec::*;
use deku::prelude::*;
use std::fs::File;
use std::ops::{Add, Sub};
use std::path::Path;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

#[derive(Debug, DekuRead)]
struct RDB {
    header: Header,
    metadata: Metadata,
    database: Database,
}

#[derive(Debug, DekuRead)]
#[deku(magic = b"REDIS")]
struct Header {
    #[deku(bytes = "4")]
    rd_version: [u8; 4],
}

#[derive(Debug, DekuRead)]
struct Metadata {
    #[deku(until = "|v: &u8| *v == 0xFA")]
    start: Vec<u8>,

    #[deku(bytes = "4")]
    rd_version: [u8; 4],

    #[deku(until = "|v: &u8| *v == 0xFA")]
    values: Vec<u8>,
}

#[derive(Debug, DekuRead)]
struct Database {
    #[deku(until = "|v: &u8| *v == 0xFE")]
    start: Vec<u8>,

    index: u8,

    resize_db_flag: u8,

    // Size of the whole table
    hash_table_size: EncodedInteger,

    // The number of entries which expire within the above
    // i.e. must be <= hash_table_size
    hash_table_size_with_expiry: EncodedInteger,

    #[deku(count = "hash_table_size.value")]
    kv_pairs: Vec<KeyValuePair>,
}

#[derive(Debug, DekuRead)]
struct KeyValuePair {
    expiring_or_type_flag: u8,

    #[deku(cond = "*(expiring_or_type_flag) == 0xFD")]
    expiry_time_in_s: Option<u32>,

    #[deku(cond = "*(expiring_or_type_flag) == 0xFC")]
    expiry_time_in_ms: Option<u64>,

    #[deku(cond = "*(expiring_or_type_flag) == 0xFC || *(expiring_or_type_flag) == 0xFD")]
    _type: u8,

    key: EncodedString,
    value: EncodedString,
}

#[derive(Debug, DekuRead)]
struct EncodedInteger {
    #[deku(reader = "EncodedLength::read_length(deku::reader)")]
    length: u8,

    #[deku(reader = "EncodedLength::read_value(deku::reader, *(length))")]
    pub value: u32,
}

#[derive(Debug, DekuRead)]
struct EncodedString {
    #[deku(bytes = "1")]
    length: u8,

    #[deku(count = "length")]
    value: Vec<u8>,
}

// run --package redis-starter-rust --bin redis-starter-rust -- --dir /home/lolletsoc/devel/pets/code_crafters/redis/test --dbfilename dump.rdb

pub async fn read_rdb_from_bytes(
    bytes: &[u8],
    arc: Arc<DashMap<String, (String, Option<SystemTime>)>>,
) -> anyhow::Result<()> {
    match RDB::from_bytes((&bytes, 0)) {
        Ok((_, rdb)) => {
            rdb.database.kv_pairs.iter().for_each(|e| {
                let expire_duration;
                if let Some(ms_expiry) = e.expiry_time_in_ms {
                    expire_duration = Some(Duration::from_millis(ms_expiry));
                } else if let Some(s_expiry) = e.expiry_time_in_s {
                    expire_duration = Some(Duration::from_secs(s_expiry as u64));
                } else {
                    expire_duration = None;
                }

                arc.insert(
                    String::from_utf8(e.key.value.clone()).unwrap(),
                    (
                        String::from_utf8(e.value.value.clone()).unwrap(),
                        match expire_duration {
                            Some(dur) => Some(UNIX_EPOCH.add(dur)),
                            None => None,
                        },
                    ),
                );
            });
            Ok(())
        }
        Err(err) => {
            // Failed to parse
            Err(anyhow::anyhow!(err))
        }
    }
}

pub async fn read_rdb(
    dir: &str,
    filename: &str,
    arc: Arc<DashMap<String, (String, Option<SystemTime>)>>,
) -> anyhow::Result<()> {
    match File::open(Path::new(dir).join(filename)) {
        Ok(mut file) => {
            match RDB::from_reader((&mut file, 0)) {
                Ok((_, rdb)) => {
                    rdb.database.kv_pairs.iter().for_each(|e| {
                        let expire_duration;
                        if let Some(ms_expiry) = e.expiry_time_in_ms {
                            expire_duration = Some(Duration::from_millis(ms_expiry));
                        } else if let Some(s_expiry) = e.expiry_time_in_s {
                            expire_duration = Some(Duration::from_secs(s_expiry as u64));
                        } else {
                            expire_duration = None;
                        }

                        arc.insert(
                            String::from_utf8(e.key.value.clone()).unwrap(),
                            (
                                String::from_utf8(e.value.value.clone()).unwrap(),
                                match expire_duration {
                                    Some(dur) => Some(UNIX_EPOCH.add(dur)),
                                    None => None,
                                },
                            ),
                        );
                    });
                    Ok(())
                }
                Err(err) => {
                    // Failed to parse
                    Err(anyhow::anyhow!(err))
                }
            }
        }

        // Doesn't exist. Ignore and continue
        Err(_) => Ok(()),
    }
}

struct EncodedLength(());

impl EncodedLength {
    fn read_value<R: std::io::Read>(reader: &mut Reader<R>, length: u8) -> Result<u32, DekuError> {
        let read_bits = reader.read_bits(length as usize).expect("");
        let binding = read_bits.unwrap();

        let i = binding.load_be::<u32>();
        Ok(i)
    }
    fn read_length<R: std::io::Read>(reader: &mut Reader<R>) -> Result<u8, DekuError> {
        let result = reader.read_bits(2).unwrap().unwrap();
        let two_bit_specifier = result.as_raw_slice()[0];

        match two_bit_specifier {
            0b0000_0000 => Ok(6),
            0b0100_0000 => Ok(14),
            0b1000_0000 => {
                reader.read_bits(6)?.unwrap();
                Ok(32)
            }
            0b1100_0000 => {
                let special_format_bits = reader.read_bits(6)?.unwrap().as_raw_slice()[0];
                match special_format_bits {
                    0b0000_0000 => Ok(8),
                    0b1000_0000 => Ok(16),
                    0b0100_0000 => Ok(32),
                    0b1100_0000 => {
                        // Complete `Compressed Strings` at some point
                        Ok(1)
                    }
                    _ => {
                        // Liam, fix this
                        Ok(0)
                    }
                }
            }
            _ => {
                // Fix later
                Ok(0)
            }
        }
    }
}

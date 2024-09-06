use dashmap::DashMap;
use deku::bitvec::*;
use deku::prelude::*;
use std::fs::File;
use std::path::Path;
use std::sync::Arc;
use std::time::SystemTime;

#[derive(Debug, DekuRead, DekuWrite)]
struct RDB {
    header: Header,
    metadata: Metadata,
    database: Database,
}

#[derive(Debug, DekuRead, DekuWrite)]
#[deku(magic = b"REDIS")]
struct Header {
    #[deku(bytes = "4")]
    rd_version: [u8; 4],
}

#[derive(Debug, DekuRead, DekuWrite)]
struct Metadata {
    #[deku(until = "|v: &u8| *v == 0xFA")]
    start: Vec<u8>,

    #[deku(bytes = "4")]
    rd_version: [u8; 4],

    #[deku(until = "|v: &u8| *v == 0xFA")]
    values: Vec<u8>,
}

#[derive(Debug, DekuRead, DekuWrite)]
struct Database {
    #[deku(until = "|v: &u8| *v == 0xFE")]
    start: Vec<u8>,

    #[deku(bytes = "1")]
    index: u8,

    flag: u8,

    hash_table_size: EncodedInteger,
    hash_table_size_with_expiry: EncodedInteger,

    #[deku(count = "hash_table_size_with_expiry.value")]
    expiring_entries: Vec<ExpiringKeyValuePair>,

    #[deku(count = "hash_table_size.value")]
    non_expiring_entries: Vec<NonExpiringKeyValuePair>,
}

#[derive(Debug, DekuRead, DekuWrite)]
struct ExpiringKeyValuePair {
    flag: u8,

    #[deku(cond = "*flag == 0xFD")]
    expiry_time_in_s: Option<u32>,

    #[deku(cond = "*flag == 0xFC")]
    expiry_time_in_ms: Option<u64>,

    kv: NonExpiringKeyValuePair,
}

#[derive(Debug, DekuRead, DekuWrite)]
struct NonExpiringKeyValuePair {
    _type: u8,
    key: EncodedString,
    value: EncodedString,
}

#[derive(Debug, DekuRead, DekuWrite)]
struct EncodedInteger {
    #[deku(reader = "EncodedLength::read_length(deku::reader)")]
    length: u8,

    #[deku(reader = "EncodedLength::read_value(deku::reader, *(length))")]
    pub value: u32,
}

#[derive(Debug, DekuRead, DekuWrite)]
struct EncodedString {
    #[deku(bytes = "1", update = "self.value.len()")]
    length: u8,

    #[deku(count = "length")]
    value: Vec<u8>,
}

// run --package redis-starter-rust --bin redis-starter-rust -- --dir /home/lolletsoc/devel/pets/code_crafters/redis/test --dbfilename dump.rdb

pub async fn read_rdb(
    dir: &str,
    filename: &str,
    arc: Arc<DashMap<String, (String, Option<SystemTime>)>>,
) -> anyhow::Result<()> {
    match File::open(Path::new(dir).join(filename)) {
        Ok(mut file) => {
            match RDB::from_reader((&mut file, 0)) {
                Ok((_, rdb)) => {
                    rdb.database.expiring_entries.iter().for_each(|e| {
                        arc.insert(
                            String::from_utf8(e.kv.key.value.clone()).unwrap(),
                            (String::from_utf8(e.kv.value.value.clone()).unwrap(), None),
                        );
                    });

                    rdb.database.non_expiring_entries.iter().for_each(|e| {
                        arc.insert(
                            String::from_utf8(e.key.value.clone()).unwrap(),
                            (String::from_utf8(e.value.value.clone()).unwrap(), None),
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

struct EncodedLength(u32);

impl EncodedLength {
    fn read_value<R: std::io::Read>(reader: &mut Reader<R>, length: u8) -> Result<u32, DekuError> {
        let read_bits = reader.read_bits(length as usize).expect("");
        let binding = read_bits.unwrap();

        Ok(binding.load_be::<u32>())
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

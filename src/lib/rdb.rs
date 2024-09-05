use deku::prelude::*;
use std::fs::File;
use std::path::Path;

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
#[deku(magic = b"FA")]
struct Metadata {
    #[deku(bytes = "4")]
    rd_version: [u8; 4],

    #[deku(until = "|v: &u8| *v == 0xFA")]
    values: Vec<u8>,
}

#[derive(Debug, DekuRead, DekuWrite)]
#[deku(magic = b"FE")]
struct Database {
    #[deku(bytes = "2")]
    index: u8,

    hash_table_size: EncodedInteger,
    hash_table_size_with_expiry: EncodedInteger,

    #[deku(count = "hash_table_size_with_expiry.value")]
    expiring_entries: Vec<KeyValuePair>,
}

#[derive(Debug, DekuRead, DekuWrite)]
#[deku(magic = b"FD")]
struct KeyValuePair {
    flag: u8,

    #[deku(cond = "*flag == 0xFD")]
    expiry_time_in_s: Option<u32>,

    #[deku(cond = "*flag == 0xFC")]
    expiry_time_in_ms: Option<u32>,

    _type: u8,
    key: EncodedString,
    value: EncodedString,
}

#[derive(Debug, DekuRead, DekuWrite)]
#[deku(magic = b"FB")]
struct EncodedInteger {
    #[deku(reader = "EncodedLength::read(deku::reader)")]
    length: u32,

    #[deku(count = "length")]
    pub value: Vec<u8>,
}

#[derive(Debug, DekuRead, DekuWrite)]
struct EncodedString {
    #[deku(bytes = "2", update = "self.value.len()")]
    length: u16,

    #[deku(count = "length")]
    value: Vec<u8>,
}

pub async fn read_rdb(dir: &str, filename: &str) -> anyhow::Result<()> {
    match File::open(Path::new(dir).join(filename)) {
        Ok(mut file) => {
            let (_, rdb) = RDB::from_reader((&mut file, 0)).unwrap();
            // Liam, finish this
            Ok(())
        }
        Err(_) => Ok(()),
    }
}

struct EncodedLength(u32);

impl EncodedLength {
    fn read<R: std::io::Read>(reader: &mut Reader<R>) -> Result<u32, DekuError> {
        let result = reader.read_bits(2).unwrap().unwrap();

        let two_bit_specifier = result.as_raw_slice()[0];
        let mut four_bytes = [0u8; 4];

        match two_bit_specifier {
            0b0000_0000 => Ok(reader.read_bits(6)?.unwrap().as_raw_slice()[0] as u32),
            0b0000_0001 => Ok(reader.read_bits(14)?.unwrap().as_raw_slice()[0] as u32),
            0b0000_0010 => {
                reader.read_bits(6)?.unwrap();

                reader
                    .read_bytes(4, &mut four_bytes)
                    .expect("liam, complete this");
                Ok(u32::from_be_bytes(four_bytes))
            }
            0b0000_0011 => {
                let special_format_bits = reader.read_bits(6)?.unwrap().as_raw_slice()[0];
                match special_format_bits {
                    0b0000_0000 => {
                        reader
                            .read_bytes(1, &mut four_bytes)
                            .expect("liam, complete this");
                        Ok(u32::from_be_bytes(four_bytes))
                    }
                    0b0000_0001 => {
                        reader
                            .read_bytes(2, &mut four_bytes)
                            .expect("liam, complete this");
                        Ok(u32::from_be_bytes(four_bytes))
                    }
                    0b0000_0010 => {
                        reader
                            .read_bytes(4, &mut four_bytes)
                            .expect("liam, complete this");
                        Ok(u32::from_be_bytes(four_bytes))
                    }
                    0b0000_0011 => {
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

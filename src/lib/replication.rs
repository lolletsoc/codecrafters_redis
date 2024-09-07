use rand::distr::Alphanumeric;
use rand::Rng;

pub struct MasterReplicationInfo {
    pub replid: String,
    pub repl_offset: u8,
}

impl MasterReplicationInfo {
    pub fn new() -> MasterReplicationInfo {
        let replid: String = rand::thread_rng()
            .sample_iter(&Alphanumeric)
            .take(40)
            .map(char::from)
            .collect();

        MasterReplicationInfo {
            replid,
            repl_offset: 0,
        }
    }
}

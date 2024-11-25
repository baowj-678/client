use std::collections::HashMap;
use dragonfly_client_config::dfdaemon::{HostStatus, HostsStatus};

pub struct HostStatusCollector {
    status: HashMap<String, u32>,
}

impl HostStatusCollector {
    pub fn new(host_status: Vec<HostStatus>) -> HostStatusCollector {
        let mut s: HashMap<String, u32> = HashMap::new();
        host_status.iter().for_each(|host|
            {s.insert(host.host_ip.to_string(), host.bandwidth);});
        HostStatusCollector{status: s}
    }

    pub fn get_host_status(&self, host: String) -> u32 {
        println!("status: {:?}, host: {:?}", self.status, host);
        return match self.status.get(&host) {
            None => 99,
            value => *value.unwrap(),
        }
    }
}
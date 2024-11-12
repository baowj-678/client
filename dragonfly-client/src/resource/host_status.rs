use std::collections::HashMap;
use dragonfly_client_config::dfdaemon::{HostStatus, HostsStatus};

pub struct HostStatusCollector {
    status: HashMap<String, u32>,
}

impl HostStatusCollector {
    fn new() -> HostStatusCollector {
        HostStatusCollector{status: HashMap::new()}
    }

    pub fn init_status(&mut self, host_status: HostsStatus) {
        host_status.hosts.iter().for_each(|host|
            {self.status.insert(host.host_ip.to_string(), host.bandwidth);})
    }

    pub fn get_host_status(&self, host: String) -> u32 {
        self.status.get(&host).unwrap().clone()
    }
}
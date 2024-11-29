use dragonfly_client_config::dfdaemon::HostStatus;
use std::collections::HashMap;
use crate::resource::piece_collector::CollectedParent;

pub struct ParentStatusSyncer {
    status: HashMap<String, u32>,
}

impl ParentStatusSyncer {
    pub fn new(host_status: Vec<HostStatus>) -> ParentStatusSyncer {
        let mut s: HashMap<String, u32> = HashMap::new();
        host_status.iter().for_each(|host|
            {s.insert(host.ip.to_string(), host.bandwidth);});
        ParentStatusSyncer {status: s}
    }

    pub fn register_parents(&self, parents: &Vec<CollectedParent>) {
        
    }
    
    pub fn unregister_parents(&self, parents: &Vec<CollectedParent>) {
        
    }
    
    pub fn get_parent_status(&self, host: &String) -> f32 {
        println!("status: {:?}, host: {:?}", self.status, host);
        match self.status.get(host) {
            None => 99f32,
            value => *value.unwrap() as f32,
        }
    }
    
    pub fn get_parents_status(&self, parents: &Vec<CollectedParent>) -> Vec<f32> {
        let mut result = Vec::new();
        parents.iter().for_each(|parent|
            { 
                result.push( match self.status.get(&parent.host.clone().unwrap().ip) {
                    None => 0f32,
                    value => *value.unwrap() as f32,
                })
            }
        );
        result
    }
}
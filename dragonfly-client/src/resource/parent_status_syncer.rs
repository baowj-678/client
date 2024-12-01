use dragonfly_client_config::dfdaemon::{HostSelector};
use std::sync::{Arc, RwLock};
use std::thread;
use std::time::Duration;
use dashmap::{DashMap};
use dragonfly_api::common::v2::Host;
use tokio::sync::{OwnedSemaphorePermit, Semaphore};
use tokio::task::JoinSet;
use tokio::time::sleep;
use tracing::{info, instrument, Instrument};
use crate::resource::piece_collector::CollectedParent;

#[derive(Clone)]
pub struct ParentStatusElement {
    status: f32,

    reference: u32,

    parent: CollectedParent,
}


pub struct ParentStatusSyncer {
    enable: bool,

    test: bool,
    
    status: Arc<DashMap<String, ParentStatusElement>>,
    
    worker_num: usize,
    
    interval: Duration,

    mutex: Arc<RwLock<usize>>,
}

impl ParentStatusSyncer {
    pub fn new(host_selector: &HostSelector) -> ParentStatusSyncer {
        let status: DashMap<String, ParentStatusElement> = DashMap::new();
        
        host_selector.hosts.iter().for_each(|host| {
            let value = ParentStatusElement {
                status: host.bandwidth as f32,
                reference: 1,
                parent: CollectedParent {
                    id: host.ip.to_string(),
                    host: Option::from(Host {
                        id: "".to_string(),
                        r#type: 0,
                        hostname: "".to_string(),
                        ip: host.ip.to_string(),
                        port: 0,
                        download_port: 0,
                        os: "".to_string(),
                        platform: "".to_string(),
                        platform_family: "".to_string(),
                        platform_version: "".to_string(),
                        kernel_version: "".to_string(),
                        cpu: None,
                        memory: None,
                        network: None,
                        disk: None,
                        build: None,
                        scheduler_cluster_id: 0,
                        disable_shared: false,
                    })
                }
            };
            status.insert(host.ip.to_string(), value);
        });
        let status = Arc::new(status);
        
        ParentStatusSyncer {
            enable: host_selector.enable,
            test: host_selector.test,
            status: status.clone(),
            worker_num: 3,
            interval: Duration::from_secs(1),
            mutex: Arc::new(RwLock::new(0)),
        }
    }

    pub fn register_parents(&self, add_parents: &Vec<CollectedParent>) {
        let status = self.status.clone();
        let mutex = self.mutex.clone();
        info!("[baowj] before register status length: {}", status.len());
        for parent in add_parents.iter() {
            let ip = parent.host.clone().unwrap().ip;
            let tmp = mutex.write().unwrap();

            match status.get_mut(&ip) {
                None => {
                    let value = ParentStatusElement {
                        status: 100.0,
                        reference: 1,
                        parent: CollectedParent {
                            id: "".to_string(),
                            host: None,
                            // todo
                        },
                    };
                    status.insert(ip.clone(), value);
                }
                Some(mut s) => {
                    info!("[baowj] register ip: {}, ref: {}", &ip, s.reference);
                    (*s).reference += 1;
                }
            }
            drop(tmp);
        }
        info!("[baowj] after register status length: {}", status.len());
    }
    
    pub fn unregister_parents(&self, delete_parents: &Vec<CollectedParent>) {
        let status = self.status.clone();
        let mutex = self.mutex.clone();
        info!("[baowj] before unregister status length: {}", status.len());
        for parent in delete_parents.iter() {
            let ip = parent.host.clone().unwrap().ip;
            let mut remove = false;
            let tmp = mutex.write().unwrap();
            
            match status.get_mut(&ip) {
                None => {}
                Some(mut s) => {
                    (*s).reference -= 1;
                    if s.reference <= 0 {
                        remove = true;
                    }
                }
            }
            if remove {
                status.remove(&ip);
            }
            drop(tmp);

            info!("[baowj] unregister ip: {}", &ip);
        }
        info!("[baowj] after unregister status length: {}", status.len());
    }

    // enable == true && test == false
    #[instrument(skip_all)]
    pub async fn run(&self) {
        info!("[baowj] ParentStatusSyncer run");
        // enable == true && test = false, skip
        if !self.enable || self.test {
            thread::park();
        }
        info!("[baowj] ParentStatusSyncer run");
        
        let status = self.status.clone();
        let num = self.worker_num.clone();
        let mutex = self.mutex.clone();
        
        async fn sync_parent_status(
            status: Arc<DashMap<String, ParentStatusElement>>,
            parent: CollectedParent,
            permit: Arc<OwnedSemaphorePermit>,
            mutex: Arc<RwLock<usize>>,
        ) {
            info!("[baowj] ParentStatusSyncer.sync_parent_status: ip: {}", parent.host.clone().unwrap().ip);
            // todo request and update
            let mutex = mutex.read().unwrap();
            status.entry(parent.host.unwrap().ip).and_modify(|v|(*v).status = 100f32);
            drop(mutex);
            drop(permit);
        }
        
        loop {
            info!("[baowj] enter loop, parents length: {}", status.len());
            let semaphore = Arc::new(Semaphore::new(num));
            let mut join_set = JoinSet::new();
            
            for s in status.iter() {
                let parent = s.parent.clone();
                info!("[baowj] sync parent {}", parent.host.clone().unwrap().ip);
                let permit = Arc::new(semaphore.clone().acquire_owned().await.unwrap());
                
                let _ = join_set.spawn(
                    sync_parent_status(
                        status.clone(),
                        parent.clone(),
                        permit.clone(),
                        mutex.clone(),
                    )
                ).in_current_span();
            }
            // Wait for all tasks to finish.
            join_set.join_all().await;
            
            // sleep
            sleep(self.interval).await;
        }
    }
    
    pub fn get_parents_status(&self, parents: &Vec<CollectedParent>) -> Vec<f32> {
        let mut result = Vec::new();
        let status = self.status.clone();

        parents.iter().for_each(|parent|
            { 
                result.push(
                    match status.get(&parent.host.clone().unwrap().ip) {
                        None => 0f32,
                        Some(value) => {
                            let v = value.clone().status;
                            v
                        },
                })
            }
        );
        info!("[baowj] get_parent_status: {:?}", result);
        result
    }
}

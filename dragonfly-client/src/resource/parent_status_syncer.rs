use std::str::FromStr;
use dragonfly_client_config::dfdaemon::{Config};
use std::sync::{Arc, RwLock};
use std::thread;
use std::time::Duration;
use bytesize::ByteSize;
use dashmap::{DashMap};
use dragonfly_api::common::v2::Host;
use dragonfly_api::dfdaemon::v2::ParentStatusRequest;
use serde::de::Unexpected::Option;
use tokio::sync::{OwnedSemaphorePermit, Semaphore};
use tokio::task::JoinSet;
use tokio::time::sleep;
use tracing::{info, instrument, Instrument};
use dragonfly_client_util::id_generator::IDGenerator;
use crate::grpc::dfdaemon_upload::DfdaemonUploadClient;
use crate::resource::piece_collector::CollectedParent;

#[derive(Clone)]
pub struct ParentStatusElement {
    status: u64,

    reference: u32,

    parent: CollectedParent,
    
    fixed: bool,
}


pub struct ParentStatusSyncer {
    config: Arc<Config>,

    enable: bool,

    syncer_enable: bool,
    
    status: Arc<DashMap<String, ParentStatusElement>>,

    pub id_generator: Arc<IDGenerator>,
    
    worker_num: usize,
    
    interval: Duration,

    mutex: Arc<RwLock<usize>>,
}

impl ParentStatusSyncer {
    pub fn new(config: Arc<Config>, id_generator: Arc<IDGenerator>,) -> ParentStatusSyncer {
        let status: DashMap<String, ParentStatusElement> = DashMap::new();

        let status = Arc::new(status);
        
        ParentStatusSyncer {
            config: config.clone(),
            enable: config.parent_selector.enable,
            syncer_enable: config.parent_selector.syncer_enable,
            id_generator,
            status: status.clone(),
            worker_num: 3,
            interval: Duration::from_secs(1),
            mutex: Arc::new(RwLock::new(0)),
        }
    }

    #[instrument(skip_all)]
    pub fn register_parents(&self, add_parents: &Vec<CollectedParent>) {
        let status = self.status.clone();
        let mutex = self.mutex.clone();
        info!("[baowj] before register status length: {}", status.len());
        for parent in add_parents.iter() {
            let (ip,port) = match parent.host.clone() {
                None => {continue;}
                Some(host) => {(host.ip, host.port)}
            };
            let lock = mutex.write().unwrap();

            match status.get_mut(&ip) {
                None => {
                    let value = ParentStatusElement {
                        status: ByteSize::from_str("1GiB").unwrap().as_u64(),
                        reference: 1,
                        parent: parent.clone(),
                        fixed: false,
                        };
                    status.insert(ip.clone(), value);
                    info!("[baowj] add register to: {}:{}, ref: {}", &ip, &port, 1);
                },
                Some(mut status_element) => {
                    (*status_element).reference += 1;
                    info!("[baowj] update register to: {}:{}, ref: {}", &ip, &port, status_element.reference);
                }
            }
            
            drop(lock);
        }
        info!("[baowj] after register status length: {}", status.len());
    }

    #[instrument(skip_all)]#[instrument(skip_all)]
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

    // enable == true
    #[instrument(skip_all)]
    pub async fn run(&self) {
        info!("[baowj] ParentStatusSyncer run");
        if self.enable {
            self.init_test_config();
        }
        
        // enable == true && syncer_enable = true skip
        if !(self.enable && self.syncer_enable) {
            thread::park();
        }
        info!("[baowj] ParentStatusSyncer run");
        
        async fn sync_parent_status(
            config: Arc<Config>,
            host_id: String,
            peer_id: String,
            status: Arc<DashMap<String, ParentStatusElement>>,
            parent: CollectedParent,
            permit: Arc<OwnedSemaphorePermit>,
            mutex: Arc<RwLock<usize>>,
        ) {
            let host = parent.host.clone().unwrap();
            
            let client = DfdaemonUploadClient::new(config.clone(),
                                                   format!("http://{}:{}", host.ip, host.port)).await.unwrap();

            let request = ParentStatusRequest {
                host_id: host_id.clone(),
                peer_id: peer_id.clone(),
            };
            match client.sync_parent_status(request).await {
                Ok(response) => {
                    let response = response.into_inner().status;
                    let mutex = mutex.read().unwrap();
                    status.entry(parent.host.unwrap().ip).and_modify(|v| {
                        if !v.fixed {
                            info!("[baowj] sync_parent_status, update {} status: {}", host.ip, ByteSize(response));
                            (*v).status = response;
                        }
                    });
                    drop(mutex);
                }
                Err(error) => {
                    info!("[baowj] sync_parent_status, failed to connect: {}, err: {:?}", host.ip, error);
                }
            };
            drop(permit);
        }

        let status = self.status.clone();
        let num = self.worker_num.clone();
        let mutex = self.mutex.clone();
        let config = self.config.clone();
        let id_generator = self.id_generator.clone();
        let host_id = id_generator.host_id();
        let peer_id = id_generator.peer_id();
        
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
                        config.clone(),
                        host_id.clone(),
                        peer_id.clone(),
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

    pub fn init_test_config(&self) {
        info!("[baowj] init_test_config");
        let config = self.config.clone();
        let status = self.status.clone();
        config.parent_selector.hosts.iter().for_each(|host| {
            let value = ParentStatusElement {
                status: host.bandwidth as u64,
                reference: 1,
                parent: CollectedParent {
                    id: host.ip.to_string(),
                    host: Some(Host {
                        id: "".to_string(),
                        r#type: 0,
                        hostname: "".to_string(),
                        ip: host.ip.to_string(),
                        port: host.port as i32,
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
                },
                fixed: true,
            };
            status.insert(host.ip.to_string(), value);
            info!("[baowj] init_test_config insert ip: {}, port: {}", host.ip.to_string(), host.port);
        });
    }

    #[instrument(skip_all)]
    pub fn get_parents_status(&self, parents: &Vec<CollectedParent>) -> Vec<u64> {
        let mut result = Vec::new();
        let status = self.status.clone();

        parents.iter().for_each(|parent|
            { 
                result.push(
                    match status.get(&parent.host.clone().unwrap().ip) { 
                        None => ByteSize::from_str("1GiB").unwrap().as_u64(), 
                        Some(value) => {
                            value.clone().status
                        },
                })
            }
        );
        info!("[baowj] get_parent_status: {:?}", result);
        result
    }
}

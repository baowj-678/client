use std::collections::HashMap;
use std::net::IpAddr;
use std::sync::{Arc, RwLock};
use std::thread;
use std::time::{Duration, SystemTime};
use dashmap::DashMap;
use dragonfly_api::common::v2::{Build, Cpu, Disk, Host, Memory};
use sysinfo::{Networks, System};
use tokio::time::sleep;
use tracing::{info, instrument};
use dragonfly_client_config::{CARGO_PKG_RUSTC_VERSION, CARGO_PKG_VERSION, GIT_COMMIT_SHORT_HASH};
use dragonfly_client_config::dfdaemon::{Config};
use pnet::datalink;
use pnet::ipnetwork::IpNetwork;
use dragonfly_api::common::v2::Network as ApiNetwork;
use crate::grpc::dfdaemon_upload::DfdaemonUploadClient;

pub struct ParentStatusServer {
    config: Arc<Config>,

    enable: bool,

    test: bool,

    interval: Duration,

    // memory: Arc<RwLock<Memory>>,
    // 
    // cpu: Arc<RwLock<Cpu>>,

    network: Arc<Network>,

    // disk: Arc<RwLock<Disk>>,

    build: Build,
}

impl ParentStatusServer {
    pub fn new(config: Arc<Config>) -> Result<Self, String> {
        let build = Build {
            git_version: CARGO_PKG_VERSION.to_string(),
            git_commit: Some(GIT_COMMIT_SHORT_HASH.to_string()),
            go_version: None,
            rust_version: Some(CARGO_PKG_RUSTC_VERSION.to_string()),
            platform: None,
        };
        Ok(
            Self {
                config: config.clone(),
                enable: config.parent_selector.enable.clone(),
                test: config.parent_selector.test.clone(),
                interval: config.parent_selector.interval.clone(),
                // memory: Arc::new(Default::default()),
                // cpu: Arc::new(Default::default()),
                network: Arc::new(
                    Network {
                        interface_to_ips: Default::default(),
                        received: Default::default(),
                        transmitted: Default::default(),
                    }
                ),
                // disk: Arc::new(Default::default()),
                build,
        })
    }

    #[instrument(skip_all)]
    pub async fn run(&self) {
        // enable == true && test = false, skip
        if !self.enable || self.test {
            thread::park();
        }
        // Get the system information.
        let mut sys = System::new_all();
        let mut networks = Networks::new_with_refreshed_list();
        let network = self.network.clone();
        // let cpu = self.cpu.clone();
        // let memory = self.memory.clone();
        // let disk = self.disk.clone();
        let config = self.config.clone();
        
        let mut last_refresh_time = SystemTime::now();

        self.init_network();
        info!("[baowj] start loop");
        loop {
            sys.refresh_all();

            // Get the process information.
            let process = sys.process(sysinfo::get_current_pid().unwrap()).unwrap();

            // Update cpu
            // let mut cpu_lock = cpu.write().unwrap();
            // cpu_lock.logical_count = sys.physical_core_count().unwrap_or_default() as u32;
            // cpu_lock.physical_count= sys.physical_core_count().unwrap_or_default() as u32;
            // cpu_lock.percent = sys.global_cpu_usage() as f64;
            // cpu_lock.process_percent = process.cpu_usage() as f64;
            // drop(cpu_lock);

            // Update memory
            // let mut memory_lock = memory.write().unwrap();
            // memory_lock.total = sys.total_memory();
            // memory_lock.available = sys.available_memory();
            // memory_lock.used = sys.used_memory();
            // memory_lock.used_percent = (sys.used_memory() / sys.total_memory()) as f64;
            // memory_lock.process_used_percent = (process.memory() / sys.total_memory()) as f64;
            // memory_lock.free = sys.free_memory();
            // drop(memory_lock);

            // Update network
            networks.refresh();
            let new_time = SystemTime::now();
            let interval = new_time.duration_since(last_refresh_time).unwrap().as_millis()  as u64;
            last_refresh_time = new_time;

            for (interface, data) in &networks {
                // kbit per sec
                network.received.insert(interface.clone(), data.received() * 1000 / 1024 / interval);
                network.transmitted.insert(interface.clone(), data.transmitted() * 1000 / 1024 / interval);
            }

            // Update disk
            match fs2::statvfs(config.storage.dir.as_path()) {
                Ok(stats) => {
                    let total_space = stats.total_space();
                    let available_space = stats.available_space();
                    let used_space = total_space - available_space;
                    let used_percent = (used_space as f64 / (total_space) as f64) * 100.0;

                    // let mut disk_lock = disk.write().unwrap();
                    // disk_lock.total = total_space;
                    // disk_lock.free = available_space;
                    // disk_lock.used = used_space;
                    // disk_lock.used_percent = used_percent;
                    // drop(disk_lock);
                },
                Err(_) => {

                }
            };
            // sleep
            sleep(self.interval).await;
        }
    }

    fn init_network(&self) {
        // 获取所有网卡信息
        let interfaces = datalink::interfaces();
        let network = self.network.clone();

        for interface in interfaces {
            network.interface_to_ips.insert(interface.name, interface.ips);
        }
        info!("[baowj] init interface and ip: {:?}", network.interface_to_ips);
    }

    pub fn status(&self, ip: IpAddr) -> Result<Host, String> {
        let mut host: Host = Default::default();
        
        // let memory = self.memory.clone();
        // let memory = memory.read().unwrap();
        // host.memory = Option::from(memory.clone());
        // drop(memory);
        // 
        // let cpu = self.cpu.clone();
        // let cpu = cpu.read().unwrap();
        // host.cpu = Option::from(cpu.clone());
        // drop(cpu);
        // 
        // let disk = self.disk.clone();
        // let disk = disk.read().unwrap();
        // host.disk = Option::from(disk.clone());
        // drop(disk);

        host.build = Option::from(self.build.clone());

        // Network
        let network = self.network.clone();
        let mut interface_name = None;
        for interface in network.interface_to_ips.iter() {
            let mut find = false;
            for if_ip in interface.value().iter() {
                if if_ip.contains(ip) {
                    find = true;
                    break;
                }
            }
            if find {
                let _ = interface_name.insert(interface.key().clone());
                break;
            }
        }
        let config = self.config.clone();
        match interface_name {
            None => {}
            Some(name) => {
                let _ = host.network.insert(
                    ApiNetwork {
                        tcp_connection_count: 0,
                        upload_tcp_connection_count: 0,
                        location: config.host.location.clone(),
                        idc: config.host.idc.clone(),
                        download_rate: network.received.get(&name).unwrap().clone(),
                        download_rate_limit: config.download.rate_limit.as_u64(),
                        upload_rate: network.transmitted.get(&name).unwrap().clone(),
                        upload_rate_limit: config.upload.rate_limit.as_u64(),
                    }
                );
            }
        }
        Ok(host)
    }

}

pub struct Network {
    interface_to_ips: DashMap<String, Vec<IpNetwork>>,

    // interface to received rate
    received: DashMap<String, u64>,

    // interface to transmitted rate
    transmitted: DashMap<String, u64>,
}

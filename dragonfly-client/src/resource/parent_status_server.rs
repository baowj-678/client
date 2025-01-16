use bytesize::ByteSize;
use dashmap::DashMap;
use dragonfly_client_config::dfdaemon::Config;
use pnet::datalink;
use pnet::ipnetwork::IpNetwork;
use std::net::IpAddr;
use std::sync::Arc;
use std::thread;
use std::time::{Duration, SystemTime};
use sysinfo::Networks;
use tokio::time::sleep;
use tracing::{info, instrument};

pub struct Network {
    interface_to_ips: DashMap<String, Vec<IpNetwork>>,

    // interface to transmitted rate
    transmitted_reserved: DashMap<String, ByteSize>,
}

pub struct ParentStatusServer {
    config: Arc<Config>,

    enable: bool,

    server_enable: bool,

    interval: Duration,

    networks: Arc<Network>,
}

impl ParentStatusServer {
    pub fn new(config: Arc<Config>) -> Result<Self, String> {
        Ok(Self {
            config: config.clone(),
            enable: config.parent_selector.enable.clone(),
            server_enable: config.parent_selector.server_enable.clone(),
            interval: config.parent_selector.interval.clone(),
            networks: Arc::new(Network {
                interface_to_ips: Default::default(),
                transmitted_reserved: Default::default(),
            }),
        })
    }

    #[instrument(skip_all)]
    pub async fn run(&self) {
        // enable == true && syncer_enable = true, skip
        if !(self.enable && self.server_enable) {
            thread::park();
        }
        // Get the system information.
        let mut networks = Networks::new_with_refreshed_list();
        let network = self.networks.clone();
        let config = self.config.clone();
        let transmitted_limited = config.parent_selector.transmitted_limit;
        let mut last_refresh_time = SystemTime::now();
        self.init_network();
        info!("[baowj] start loop");
        loop {
            info!("[baowj] parent_status_server refresh");
            // sleep
            sleep(self.interval).await;

            // Update network
            networks.refresh();
            let new_time = SystemTime::now();
            let interval = new_time
                .duration_since(last_refresh_time)
                .unwrap()
                .as_millis() as u64;
            last_refresh_time = new_time;

            for (interface, data) in &networks {
                // bit per sec
                network.transmitted_reserved.insert(
                    interface.clone(),
                    ByteSize(transmitted_limited.as_u64() - data.transmitted() * 1000 / interval),
                );
            }
        }
    }

    fn init_network(&self) {
        // 获取所有网卡信息
        let interfaces = datalink::interfaces();
        let network = self.networks.clone();
        for interface in interfaces {
            network
                .interface_to_ips
                .insert(interface.name, interface.ips);
        }
        info!(
            "[baowj] init interface and ip: {:?}",
            network.interface_to_ips
        );
    }

    pub fn status(&self, ip: IpAddr) -> Result<ByteSize, String> {
        let mut status = ByteSize(0u64);
        // get network
        let network = self.networks.clone();
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
        match interface_name {
            None => {}
            Some(name) => match network.transmitted_reserved.get(&name) {
                None => {
                    info!(
                        "[baowj] failed to get reserved transmitted of interface: {}",
                        name
                    );
                }
                Some(t) => {
                    status = t.value().clone();
                }
            },
        }
        info!(
            "[baowj] parent_status_server status, ip: {}, status:{:?}",
            ip, status
        );
        Ok(status)
    }
}

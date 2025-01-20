/*
 *     Copyright 2025 The Dragonfly Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

use crate::grpc::dfdaemon_upload::DfdaemonUploadClient;
use crate::resource::piece_collector::CollectedParent;
use crate::shutdown::Shutdown;
use bytesize::ByteSize;
use dashmap::DashMap;
use dragonfly_api::common::v2::Host;
use dragonfly_api::dfdaemon::v2::SyncHostRequest;
use dragonfly_client_config::dfdaemon::Config;
use dragonfly_client_core::error::DFError::TaskNotFound;
use dragonfly_client_core::error::{ErrorType, OrErr};
use dragonfly_client_core::Error;
use dragonfly_client_core::Result;
use dragonfly_client_util::id_generator::IDGenerator;
use lru::LruCache;
use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};
use std::num::NonZeroUsize;
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio_stream::StreamExt;
use tracing::{debug, error, info, instrument, Instrument};
use validator::HasLen;

const DEFAULT_AVAILABLE_CAPACITY: f64 = ByteSize::gb(10).as_u64() as f64;

const DEFAULT_SYNC_HOST_TIMEOUT: u32 = 5;

/// TaskParentSelector is used to store data to select parents for specific task.
#[derive(Clone)]
pub struct TaskParentSelector {
    /// parents is the latest host info of different parents.
    parents: DashMap<String, Host>,

    /// parent_list is the parent_id corresponding to probability.
    parent_list: Vec<String>,

    /// probability is the selection probability of different parents.
    probability: Vec<f64>,

    /// last_sync_time records the latest time for refreshing probability.
    last_sync_time: SystemTime,

    /// sync_interval represents the time interval between two refreshing probability operations.
    sync_interval: Duration,

    /// rng is a random number generator.
    rng: StdRng,
}

/// TaskParentSelector implements the task parent selector.
impl TaskParentSelector {
    /// new create a TaskParentSelector.
    pub fn new(
        collected_parents: Vec<CollectedParent>,
        sync_interval: Duration,
    ) -> TaskParentSelector {
        let parents: DashMap<String, Host> = DashMap::new();
        let mut parent_list: Vec<String> = Vec::new();
        let mut probability: Vec<f64> = Vec::new();

        collected_parents.iter().for_each(|parent| {
            parents.insert(parent.id.clone(), Host::default());
            parent_list.push(parent.id.clone());
            probability.push(1f64 / parents.len() as f64);
        });

        TaskParentSelector {
            parents,
            parent_list,
            probability,
            last_sync_time: SystemTime::UNIX_EPOCH,
            sync_interval,
            rng: StdRng::seed_from_u64(
                SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .unwrap()
                    .as_secs(),
            ),
        }
    }

    /// select_parent return an optimal parent.
    pub fn select_parent(&mut self) -> String {
        // Get now time.
        let now_time = SystemTime::now();

        // Lazy refresh probability.
        if now_time.duration_since(self.last_sync_time).unwrap() > self.sync_interval {
            let mut parent_available_capacity = Vec::with_capacity(self.probability.len());
            let mut count = 0;
            let mut sum = 0f64;
            let parent_map = self.parents.clone();

            self.parent_list
                .iter()
                .for_each(|parent_id| match parent_map.get(parent_id) {
                    None => {
                        parent_available_capacity.push(0f64);
                    }
                    Some(host) => match Self::available_capacity(host.value().clone()) {
                        Ok(capacity) => {
                            parent_available_capacity.push(capacity);
                            sum += capacity;
                            count += 1;
                        }
                        Err(_) => {
                            parent_available_capacity.push(0f64);
                        }
                    },
                });
            // Calc average available capacity.
            let mut avg = DEFAULT_AVAILABLE_CAPACITY;
            if count != 0 {
                avg = sum / count as f64;
            }
            // Calc sum.
            sum += avg * (parent_available_capacity.len() - count) as f64;

            // Prevent division by 0
            sum += 0.1f64;

            // Update probability.
            self.probability
                .iter_mut()
                .enumerate()
                .for_each(|(idx, p)| {
                    if parent_available_capacity[idx] == 0f64 {
                        *p = avg / sum;
                    } else {
                        *p = parent_available_capacity[idx] / sum;
                    }
                });
            debug!("update probability to {:?}", self.probability);
            info!("[baowj] update probability to {:?}", self.probability);
            // Reset last_sync_time.
            self.last_sync_time = now_time;
        }

        // Get random value.
        let random_num: f64 = self.rng.gen();
        // Return the first parent_id that the sum is bigger than random value.
        let mut sum: f64 = 0f64;
        for (idx, v) in self.probability.iter().enumerate() {
            sum += v;
            if sum >= random_num {
                return self.parent_list[idx].clone();
            }
        }
        self.parent_list[self.parent_list.len() - 1].clone()
    }

    /// available_capacity return the available capacity of the host.
    fn available_capacity(host: Host) -> Result<f64> {
        match host.network {
            None => Ok(DEFAULT_AVAILABLE_CAPACITY),
            Some(network) => Ok(network.upload_rate as f64),
        }
    }
}

/// ParentSelector represents a parent selector.
#[derive(Clone)]
pub struct ParentSelector {
    /// config is the configuration of the dfdaemon.
    config: Arc<Config>,

    /// sync_interval represents the time interval between two refreshing probability operations.
    sync_interval: Duration,

    /// tasks is the collector for all parent selection tasks.
    tasks: Arc<DashMap<String, TaskParentSelector>>,

    /// parent_cache is the lru cache to store sync host thread.
    parent_cache: Arc<Mutex<LruCache<String, Shutdown>>>,

    /// id_generator is a IDGenerator.
    id_generator: Arc<IDGenerator>,
}

/// TaskParentSelector implements the task parent selector.
impl ParentSelector {
    /// new returns a ParentSelector.
    #[instrument(skip_all)]
    pub fn new(config: Arc<Config>, id_generator: Arc<IDGenerator>) -> ParentSelector {
        let config = config.clone();
        let sync_interval = config.download.parent_selector.sync_interval;
        let tasks = Arc::new(DashMap::new());
        let parent_cache = LruCache::new(
            NonZeroUsize::try_from(config.download.parent_selector.capacity).unwrap(),
        );
        let id_generator = id_generator.clone();

        ParentSelector {
            config,
            sync_interval,
            tasks,
            parent_cache: Arc::new(Mutex::new(parent_cache)),
            id_generator,
        }
    }

    /// register_parents registers task and it's parents.
    #[instrument(skip_all)]
    pub fn register_parents(&self, task_id: String, add_parents: &Vec<CollectedParent>) {
        info!("[baowj] register");
        // If not enable.
        if !self.config.download.parent_selector.enable {
            info!("[baowj] parent selector disabled");
            return;
        }

        // No parents.
        if add_parents.length() == 0 {
            return;
        }
        // Get tasks
        let tasks = self.tasks.clone();

        // Add task
        let task = TaskParentSelector::new(add_parents.clone(), self.sync_interval);
        tasks.insert(task_id, task);

        // Get LRU cache.
        let cache = self.parent_cache.clone();
        let cache = cache.lock();
        let config = self.config.clone();

        if let Ok(mut cache) = cache {
            for parent in add_parents {
                let shutdown = Shutdown::new();

                if cache.len() == cache.cap().get() {
                    if let Some((_, shutdown)) = cache.pop_lru() {
                        // Stop popped thread.
                        shutdown.trigger();
                    }
                }
                cache.push(parent.id.clone(), shutdown.clone());

                // Start new thread.
                let config = config.clone();
                let host_id = self.id_generator.host_id();
                let peer_id = self.id_generator.peer_id();
                let parent = parent.clone();
                let tasks = self.tasks.clone();
                let shutdown = shutdown.clone();
                let sync_host_timeout =
                    config.download.parent_selector.sync_interval * DEFAULT_SYNC_HOST_TIMEOUT;
                tokio::spawn(
                    async move {
                        let _ = Self::sync_host(
                            config,
                            host_id,
                            peer_id,
                            parent,
                            tasks,
                            shutdown,
                            sync_host_timeout,
                        )
                        .await;
                    }
                    .in_current_span(),
                );
            }
        }
    }

    /// unregister_parents unregisters task.
    #[instrument(skip_all)]
    pub fn unregister_parents(&self, task_id: String) {
        let tasks = self.tasks.clone();
        tasks.remove(&task_id);
    }

    /// sync_host is a sub thread to sync host info from the parent.
    #[allow(clippy::too_many_arguments)]
    #[instrument(skip_all)]
    async fn sync_host(
        config: Arc<Config>,
        host_id: String,
        peer_id: String,
        parent: CollectedParent,
        tasks: Arc<DashMap<String, TaskParentSelector>>,
        shutdown: Shutdown,
        sync_host_timeout: Duration,
    ) -> Result<()> {
        info!("[baowj] sync host info from parent {}", parent.id);

        // If parent.host is None, skip it.
        let host = parent.host.clone().ok_or_else(|| {
            error!("peer {:?} host is empty", parent);
            Error::InvalidPeer(parent.id.clone())
        })?;
        // Create a dfdaemon upload client.
        let dfdaemon_upload_client =
            DfdaemonUploadClient::new(config, format!("http://{}:{}", host.ip, host.port))
                .await
                .inspect_err(|err| {
                    error!(
                        "create dfdaemon upload client from parent {} failed: {}",
                        parent.id, err
                    );
                })
                .unwrap();
        info!("[baowj] sync_host get client");

        let response = dfdaemon_upload_client
            .sync_host(SyncHostRequest { host_id, peer_id })
            .await
            .inspect_err(|err| {
                error!("sync host info from parent {} failed: {}", parent.id, err);
            })
            .unwrap();
        info!("[baowj] sync_host get response");
        // If the response repeating timeout exceeds the piece download timeout,
        // the stream will return error.
        let out_stream = response.into_inner().timeout(sync_host_timeout);
        tokio::pin!(out_stream);

        // Get tasks.
        let tasks = tasks.clone();
        while let Some(message) = out_stream.try_next().await.or_err(ErrorType::StreamError)? {
            // Check shutdown.
            if shutdown.is_shutdown() {
                break;
            }
            // Deal with massage.
            match message {
                Ok(message) => {
                    info!(
                        "[baowj] sync_host info: {:?} from parent {}",
                        message, parent.id
                    );
                    // Update the parent host information for all tasks associated with this parent.
                    tasks.iter_mut().for_each(|task| {
                        if let Some(mut parent_info) = task.parents.get_mut(&parent.id) {
                            *parent_info = message.clone();
                        }
                    });
                }
                Err(err) => {
                    // Err, return
                    info!("sync host info from parent {} error {}", parent.id, err);
                    break;
                }
            }
        }
        Ok(())
    }

    /// optimal_parent get optimal parent for the task.
    #[instrument(skip_all)]
    pub fn optimal_parent(&self, task_id: String) -> Result<String> {
        let tasks = self.tasks.clone();
        match tasks.clone().get_mut(&task_id) {
            None => Err(TaskNotFound(task_id)),
            Some(mut task) => {
                let parent_id = task.select_parent().clone();
                debug!("get optimal parent {}", parent_id);
                info!("[baowj] get optimal parent {}", parent_id);
                Ok(parent_id)
            }
        }
    }
}

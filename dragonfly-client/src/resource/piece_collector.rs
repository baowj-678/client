/*
 *     Copyright 2023 The Dragonfly Authors
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
use dashmap::{DashMap, DashSet};
use dragonfly_api::common::v2::Host;
use dragonfly_api::dfdaemon::v2::SyncPiecesRequest;
use dragonfly_client_config::dfdaemon::Config;
use dragonfly_client_core::error::{ErrorType, OrErr};
use dragonfly_client_core::{Error, Result};
use dragonfly_client_storage::metadata;
use std::sync::Arc;
use tokio::sync::mpsc::{self, Receiver, Sender};
use tokio::task::JoinSet;
use tokio_stream::StreamExt;
use tracing::{error, info, instrument, Instrument};
use std::collections::VecDeque;
use std::collections::HashMap;
use crossbeam_queue::SegQueue;
use std::thread;
use std::time::Duration;
// use openssl::rand;
use rand::Rng;
use crate::metrics::collect_backend_request_failure_metrics;
use crate::resource::host_status::HostStatusCollector;

/// CollectedParent is the parent peer collected from the remote peer.
#[derive(Clone, Debug)]
pub struct CollectedParent {
    /// id is the id of the parent.
    pub id: String,

    /// host is the host of the parent.
    pub host: Option<Host>,
}

/// CollectedPiece is the piece collected from a peer.
pub struct CollectedPiece {
    /// number is the piece number.
    pub number: u32,

    /// length is the piece length.
    pub length: u64,

    /// parent is the parent peer.
    pub parent: CollectedParent,
}

/// PieceCollector is used to collect pieces from peers.
pub struct PieceCollector {
    /// config is the configuration of the dfdaemon.
    config: Arc<Config>,

    /// host_id is the id of the host.
    host_id: String,

    /// task_id is the id of the task.
    task_id: String,

    /// parents is the parent peers.
    parents: Vec<CollectedParent>,

    /// parents status
    parents_status: Vec<f32>,

    /// interested_pieces is the pieces interested by the collector.
    interested_pieces: Vec<metadata::Piece>,

    /// collected_pieces is the pieces collected from peers.
    collected_pieces: Arc<DashSet<u32>>,

    // collected_parent id -> collected_pieces
    waited_pieces: Arc<HashMap<String, SegQueue<CollectedPiece>>>,

    collector: HostStatusCollector,

    next_idx: usize,

    rng: rand
}

impl PieceCollector {
    /// new creates a new PieceCollector.
    #[instrument(skip_all)]
    pub fn new(
        config: Arc<Config>,
        host_id: &str,
        task_id: &str,
        interested_pieces: Vec<metadata::Piece>,
        parents: Vec<CollectedParent>,
        collector: HostStatusCollector,
    ) -> Self {
        let collected_pieces = Arc::new(DashSet::new());
        let mut waited_pieces: Arc<HashMap<String, SegQueue<CollectedPiece>>> = Arc::new(HashMap::new());
        while let Some(&parent) = parents.iter().next() {
            waited_pieces.insert(parent.id, SegQueue::new());
        }
        let parents_status = Vec::new();
        let mut rng = rand::thread_rng();

        Self {
            config,
            task_id: task_id.to_string(),
            host_id: host_id.to_string(),
            parents,
            parents_status,
            interested_pieces,
            collected_pieces,
            waited_pieces,
            collector,
            next_idx: 0,
            rng,
        }
    }

    /// run runs the piece collector.
    #[instrument(skip_all)]
    pub async fn run(&self) {
        let config = self.config.clone();
        let host_id = self.host_id.clone();
        let task_id = self.task_id.clone();
        let parents = self.parents.clone();
        let interested_pieces = self.interested_pieces.clone();
        let collected_pieces = self.collected_pieces.clone();
        let waited_pieces = self.waited_pieces.clone();
        let collected_piece_timeout = self.config.download.piece_timeout;
        tokio::spawn(
            async move {
                Self::collect_from_remote_peers(
                    config,
                    host_id,
                    task_id,
                    parents,
                    interested_pieces,
                    collected_pieces,
                    waited_pieces,
                    collected_piece_timeout,
                )
                .await
                .unwrap_or_else(|err| {
                    error!("collect pieces failed: {}", err);
                });
            }
            .in_current_span(),
        );
    }

    /// collect_from_remote_peers collects pieces from remote peers.
    #[allow(clippy::too_many_arguments)]
    #[instrument(skip_all)]
    async fn collect_from_remote_peers(
        config: Arc<Config>,
        host_id: String,
        task_id: String,
        parents: Vec<CollectedParent>,
        interested_pieces: Vec<metadata::Piece>,
        collected_pieces: Arc<DashSet<u32>>,
        waited_pieces: Arc<HashMap<String, SegQueue<CollectedPiece>>>,
        collected_piece_timeout: Duration,
    ) -> Result<()> {
        // Create a task to collect pieces from peers.
        let mut join_set = JoinSet::new();
        for parent in parents.iter() {
            #[allow(clippy::too_many_arguments)]
            async fn sync_pieces(
                config: Arc<Config>,
                host_id: String,
                task_id: String,
                parent: CollectedParent,
                parents: Vec<CollectedParent>,
                interested_pieces: Vec<metadata::Piece>,
                waited_pieces: Arc<HashMap<String, SegQueue<CollectedPiece>>>,
                collected_piece_timeout: Duration,
            ) -> Result<CollectedParent> {
                info!("sync pieces from parent {}", parent.id);

                // If candidate_parent.host is None, skip it.
                let host = parent.host.clone().ok_or_else(|| {
                    error!("peer {:?} host is empty", parent);
                    Error::InvalidPeer(parent.id.clone())
                })?;

                // Create a dfdaemon client.
                let dfdaemon_upload_client =
                    DfdaemonUploadClient::new(config, format!("http://{}:{}", host.ip, host.port))
                        .await
                        .map_err(|err| {
                            error!(
                                "create dfdaemon upload client from parent {} failed: {}",
                                parent.id, err
                            );
                            err
                        })?;

                let response = dfdaemon_upload_client
                    .sync_pieces(SyncPiecesRequest {
                        host_id: host_id.to_string(),
                        task_id: task_id.to_string(),
                        interested_piece_numbers: interested_pieces
                            .iter()
                            .map(|piece| piece.number)
                            .collect(),
                    })
                    .await
                    .map_err(|err| {
                        error!("sync pieces from parent {} failed: {}", parent.id, err);
                        err
                    })?;

                // If the response repeating timeout exceeds the piece download timeout, the stream will return error.
                let out_stream = response.into_inner().timeout(collected_piece_timeout);
                tokio::pin!(out_stream);

                while let Some(message) =
                    out_stream.try_next().await.or_err(ErrorType::StreamError)?
                {
                    let message = message?;

                    info!(
                        "received piece {}-{} metadata from parent {}",
                        task_id, message.number, parent.id
                    );

                    let parent = parents
                        .iter()
                        .find(|p| p.id == parent.id)
                        .ok_or_else(|| {
                            error!("parent {} not found", parent.id.as_str());
                            Error::InvalidPeer(parent.id.clone())
                        })?;

                    // add piece to waited_piece
                    let new_piece = CollectedPiece {
                        number: message.number,
                        length: message.length,
                        parent: parent.clone(),
                    };
                    let piece_queue = waited_pieces.get(&parent.host.clone().unwrap().ip.clone());
                    piece_queue.unwrap().push_back(new_piece);
                }

                Ok(parent)
            }

            join_set.spawn(
                sync_pieces(
                    config.clone(),
                    host_id.clone(),
                    task_id.clone(),
                    parent.clone(),
                    parents.clone(),
                    interested_pieces.clone(),
                    waited_pieces.clone(),
                    collected_piece_timeout,
                )
                .in_current_span(),
            );
        }

        // Wait for all tasks to finish.
        while let Some(message) = join_set.join_next().await {
            match message {
                Ok(Ok(peer)) => {
                    info!("peer {} sync pieces finished", peer.id);

                    // If all pieces are collected, abort all tasks. todo
                    if collected_pieces.len() == interested_pieces.len() {
                        info!("all pieces are collected, abort all tasks");
                        join_set.abort_all();
                    }
                }
                Ok(Err(err)) => {
                    error!("sync pieces failed: {}", err);
                }
                Err(err) => {
                    error!("sync pieces failed: {}", err);
                }
            }
        }

        Ok(())
    }

    // todo
    #[instrument(skip_all)]
    pub fn next_piece(&mut self) -> CollectedPiece {
        let p = self.get_parent_ip();
        let mut queue = self.waited_pieces.get(&p).unwrap();
        let mut piece = CollectedPiece {
            number: 0,
            length: 0,
            parent: CollectedParent { id: "".to_string(), host: None },
        };
        let mut find = false;
        let start_idx = self.next_idx;
        loop {
            loop {
                match queue.pop() {
                    Some(p) => {
                        if self.collected_pieces.contains(&p.number) {
                            continue;
                        }
                        piece = p;
                        find = true;
                        break;
                    }
                    None => {
                        break;
                    }
                }
            }
            if find {
                break;
            } else {
                let parent: CollectedParent = self.parents[self.next_idx].clone();
                queue = self.waited_pieces.get(&parent.host.clone().unwrap().ip.clone()).unwrap();
                self.next_idx = (self.next_idx + 1) % self.parents.len();
                if self.next_idx == start_idx {
                    thread::sleep(Duration::from_millis(100));
                }
            }
        }
        self.collected_pieces.insert(piece.number);
        piece
    }

    fn sync_parents_status(&mut self) {
        /// 获取状态
        self.parents_status = Vec::new();
        self.parents.iter().for_each(|p|
            {self.parents_status.push(self.collector.get_host_status(p.host.clone().unwrap().ip.to_string()) as f32)});

        /// 归一化
        let mut sum = 0.0;
        self.parents_status.iter().for_each(|p| sum += p);
        self.parents_status.iter_mut().for_each(|p|*p /= sum);
    }

    pub fn get_parent_ip(&self) -> String {
        /// 获取的parent id
        let random_num: f32 = self.rng.gen();
        let mut s: f32 = 0.0;
        for (index, v) in self.parents_status.iter().enumerate() {
            s += v;
            if s >= random_num {
                return self.parents[index].host.clone().unwrap().ip.clone()
            }
        }
        self.parents[0].host.clone().unwrap().ip.clone()
    }
}

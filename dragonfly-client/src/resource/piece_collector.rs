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
use dashmap::{DashSet};
use dragonfly_api::common::v2::Host;
use dragonfly_api::dfdaemon::v2::SyncPiecesRequest;
use dragonfly_client_config::dfdaemon::Config;
use dragonfly_client_core::error::{ErrorType, OrErr};
use dragonfly_client_core::{Error, Result};
use dragonfly_client_storage::metadata;
use std::sync::Arc;
use tokio::task::JoinSet;
use tokio_stream::StreamExt;
use tracing::{error, info, instrument, Instrument};
use crossbeam_queue::SegQueue;
use std::thread;
use std::time::Duration;
// use openssl::rand;
use rand::{Rng, SeedableRng};
use rand::rngs::StdRng;
use crate::resource::host_status::ParentStatusSyncer;

/// CollectedParent is the parent peer collected from the remote peer.
#[derive(Clone, Debug)]
pub struct CollectedParent {
    /// id is the id of the parent.
    pub id: String,

    /// host is the host of the parent.
    pub host: Option<Host>,
}

/// CollectedPiece is the piece collected from a peer.
#[derive(Clone)]
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
    waited_queues: Arc<Vec<Arc<SegQueue<CollectedPiece>>>>,

    syncer: ParentStatusSyncer,

    next_idx: usize,

    rng: StdRng,
    
    enable_parent_selection: bool,
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
        collector: ParentStatusSyncer,
    ) -> Self {
        let collected_pieces = Arc::new(DashSet::new());
        let mut waited_pieces: Vec<Arc<SegQueue<CollectedPiece>>> = Vec::new();
        for _ in parents.iter() {
            waited_pieces.push(Arc::new(SegQueue::new()));
        }
        let waited_pieces = Arc::new(waited_pieces);
        let parents_status = Vec::new();
        let seed: u64 = 42;
        let rng = StdRng::seed_from_u64(seed);
        let enable_host_selection = config.host_selector.enable.clone();

        Self {
            config,
            task_id: task_id.to_string(),
            host_id: host_id.to_string(),
            parents,
            parents_status,
            interested_pieces,
            collected_pieces,
            waited_queues: waited_pieces,
            syncer: collector,
            next_idx: 0,
            rng,
            enable_parent_selection: enable_host_selection,
        }
    }

    /// run runs the piece collector.
    #[instrument(skip_all)]
    pub async fn run(&mut self) {
        info!("[baowj] enter PieceCollector.run()");
        let config = self.config.clone();
        let host_id = self.host_id.clone();
        let task_id = self.task_id.clone();
        let parents = self.parents.clone();
        let interested_pieces = self.interested_pieces.clone();
        let collected_pieces = self.collected_pieces.clone(); 
        let waited_pieces = self.waited_queues.clone();
        let collected_piece_timeout = self.config.download.piece_timeout.clone();
        self.init_parents_status();
        info!("[baowj] after sync parent status");
        tokio::spawn(
            async move {
                info!("[baowj] enter PieceCollector.run() async move");
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
                info!("[baowj] leave PieceCollector.run() async move");
            }
            .in_current_span()
        );
        info!("[baowj] leave PieceCollector.run()");
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
        waited_pieces: Arc<Vec<Arc<SegQueue<CollectedPiece>>>>,
        collected_piece_timeout: Duration,
    ) -> Result<()> {
        // Create a task to collect pieces from peers.
        info!("[baowj] start collect from remote peers");
        let mut join_set = JoinSet::new();
        for (i, parent) in parents.iter().enumerate() {
            #[allow(clippy::too_many_arguments)]
            async fn sync_pieces(
                config: Arc<Config>,
                host_id: String,
                task_id: String,
                parent: CollectedParent,
                interested_pieces: Vec<metadata::Piece>,
                waited_queue: Arc<SegQueue<CollectedPiece>>,
                collected_piece_timeout: Duration,
            ) -> Result<CollectedParent> {
                // If candidate_parent.host is None, skip it.
                let host = parent.host.clone().ok_or_else(|| {
                    error!("peer {:?} host is empty", parent);
                    Error::InvalidPeer(parent.id.clone())
                })?;
                info!("[baowj] sync pieces from parent {}, {}:{}", parent.id, host.ip, host.port);

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
                    
                    // add piece to waited_piece
                    let new_piece = CollectedPiece {
                        number: message.number,
                        length: message.length,
                        parent: parent.clone(),
                    };
                    waited_queue.push(new_piece);
                }

                Ok(parent)
            }

            join_set.spawn(
                sync_pieces(
                    config.clone(),
                    host_id.clone(),
                    task_id.clone(),
                    parent.clone(),
                    interested_pieces.clone(),
                    waited_pieces[i].clone(),
                    collected_piece_timeout.clone(),
                )
                .in_current_span(),
            );
        }

        // Wait for all tasks to finish.
        while let Some(message) = join_set.join_next().await {
            match message {
                Ok(Ok(peer)) => {
                    info!("peer {} sync pieces finished", peer.id);

                    // If all pieces are collected, abort all tasks.
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

    #[instrument(skip_all)]
    pub fn next_piece(&mut self) -> CollectedPiece {
        let mut piece = CollectedPiece {
            number: 0,
            length: 0,
            parent: CollectedParent { id: "".to_string(), host: None },
        };
        let waited_pieces = self.waited_queues.clone();
        let collected_pieces = self.collected_pieces.clone();
        let mut find = false;
        
        if self.enable_parent_selection {
            let idx = self.random_parent_idx();
            info!("[baowj] next_piece get random idx: {}", idx);

            let queue = waited_pieces[idx].clone();
            loop {
                match queue.pop() {
                    Some(cp) => {
                        if collected_pieces.contains(&cp.number) {
                            continue;
                        }
                        piece = cp;
                        find = true;
                        break
                    }
                    None => {
                        break;
                    }
                }
            }
            if find {
                collected_pieces.insert(piece.number);
                return piece;
            }
            info!("[baowj] parent selection failed");
        }
        
        let start_idx = self.next_idx;
        loop {
            let queue = waited_pieces[start_idx].clone();
            self.next_idx = (self.next_idx + 1) % self.parents.len();
            loop {
                match queue.pop() {
                    Some(cp) => {
                        if collected_pieces.contains(&cp.number) {
                            continue;
                        }
                        piece = cp;
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
                if self.next_idx == start_idx {
                    info!("[baowj] sleep");
                    thread::sleep(Duration::from_millis(500));
                }
            }
        }
        collected_pieces.insert(piece.number);
        piece
    }

    fn init_parents_status(&mut self) {
        // 获取状态
        self.parents_status = Vec::new();

        if self.enable_parent_selection {
            // 注册 syncer
            self.register_parents();
            // 初始化
            let parents_status = self.syncer.get_parents_status(&self.parents);
            parents_status.iter().for_each(|status| self.parents_status.push(*status));
        } else {
            // 全部初始化为 100
            self.parents.iter().for_each(|_| self.parents_status.push(100f32));
        }

        // 归一化
        let mut sum = 0.0;
        self.parents_status.iter().for_each(|p| sum += p);
        self.parents_status.iter_mut().for_each(|p|*p /= sum);
        
        info!("[baowj] parents: {:?}", self.parents);
        info!("[baowj] sync parents status status: {:?}", self.parents_status)
    }

    fn register_parents(&self) {
        self.syncer.register_parents(&self.parents);
    }

    fn unregister_parents(&self) {
        self.syncer.unregister_parents(&self.parents);
    }

    pub fn random_parent_idx(&mut self) -> usize {
        // 获取的  parent id
        let random_num: f32 = self.rng.gen();
        let mut s: f32 = 0.0;
        for (index, v) in self.parents_status.iter().enumerate() {
            s += v;
            if s >= random_num {
                return index
            }
        }
        0
    }
    
    pub fn collected_pieces_num(&self) -> usize {
        self.collected_pieces.clone().len()
    }
}

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
use crate::resource::parent_selector::ParentSelector;
use crossbeam_queue::SegQueue;
use dashmap::DashSet;
use dragonfly_api::common::v2::Host;
use dragonfly_api::dfdaemon::v2::SyncPiecesRequest;
use dragonfly_client_config::dfdaemon::Config;
use dragonfly_client_core::error::{ErrorType, OrErr};
use dragonfly_client_core::{Error, Result};
use dragonfly_client_storage::metadata;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use tokio::task::JoinSet;
use tokio_stream::StreamExt;
use tracing::log::debug;
use tracing::{error, info, instrument, Instrument};

/// CollectedParent is the parent peer collected from the parent.
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

    /// interested_pieces is the pieces interested by the collector.
    interested_pieces: Vec<metadata::Piece>,

    /// collected_pieces is the pieces collected from peers.
    collected_pieces: Arc<DashSet<u32>>,

    /// collected_parent id -> collected_pieces
    waited_queues: Arc<Vec<Arc<SegQueue<CollectedPiece>>>>,

    parent_selector: Arc<ParentSelector>,

    parent_id: Arc<HashMap<String, usize>>,

    next_idx: usize,
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
        parent_selector: Arc<ParentSelector>,
    ) -> Self {
        let collected_pieces = Arc::new(DashSet::with_capacity(interested_pieces.len()));
        for interested_piece in &interested_pieces {
            collected_pieces.insert(interested_piece.number);
        }

        let mut waited_pieces: Vec<Arc<SegQueue<CollectedPiece>>> = Vec::new();
        let mut parent_id = HashMap::new();
        for (i, parent) in parents.iter().enumerate() {
            waited_pieces.push(Arc::new(SegQueue::new()));
            parent_id.insert(parent.id.clone(), i);
        }
        let waited_queues = Arc::new(waited_pieces);
        let parent_id = Arc::new(parent_id);
        let next_idx = 0;

        Self {
            config,
            task_id: task_id.to_string(),
            host_id: host_id.to_string(),
            parents,
            interested_pieces,
            collected_pieces,
            waited_queues,
            parent_selector,
            parent_id,
            next_idx,
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
        let collected_piece_timeout = self.config.download.piece_timeout;
        let waited_pieces = self.waited_queues.clone();

        // register
        self.register_parents();

        tokio::spawn(
            async move {
                Self::collect_from_parents(
                    config,
                    &host_id,
                    &task_id,
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

    /// collect_from_parents collects pieces from parents.
    #[allow(clippy::too_many_arguments)]
    #[instrument(skip_all)]
    async fn collect_from_parents(
        config: Arc<Config>,
        host_id: &str,
        task_id: &str,
        parents: Vec<CollectedParent>,
        interested_pieces: Vec<metadata::Piece>,
        collected_pieces: Arc<DashSet<u32>>,
        waited_pieces: Arc<Vec<Arc<SegQueue<CollectedPiece>>>>,
        collected_piece_timeout: Duration,
    ) -> Result<()> {
        // Create a task to collect pieces from peers.
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
                        .inspect_err(|err| {
                            error!(
                                "create dfdaemon upload client from parent {} failed: {}",
                                parent.id, err
                            );
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
                    .inspect_err(|err| {
                        error!("sync pieces from parent {} failed: {}", parent.id, err);
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
                    host_id.to_string(),
                    task_id.to_string(),
                    parent.clone(),
                    interested_pieces.clone(),
                    waited_pieces[i].clone(),
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

                    // If all pieces are collected, abort all tasks.
                    if collected_pieces.is_empty() {
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
    pub async fn next_piece(&mut self) -> CollectedPiece {
        let mut piece = CollectedPiece {
            number: 0,
            length: 0,
            parent: CollectedParent {
                id: "".to_string(),
                host: None,
            },
        };
        let waited_pieces = self.waited_queues.clone();
        let collected_pieces = self.collected_pieces.clone();
        let parent_selector = self.parent_selector.clone();
        let parent_ids = self.parent_id.clone();

        let mut find = false;

        if let Ok(id) = parent_selector.optimal_parent(self.task_id.clone()) {
            let idx = *parent_ids.get(&id).unwrap();
            let queue = waited_pieces[idx].clone();
            while let Some(cp) = queue.pop() {
                if collected_pieces.contains(&cp.number) {
                    piece = cp;
                    find = true;
                    break;
                }
            }
            if find {
                collected_pieces.remove(&piece.number);
                debug!("collect piece {} by parent selector", piece.number);
                return piece;
            }
        }

        let start_idx = self.next_idx;
        loop {
            let queue = waited_pieces[start_idx].clone();
            self.next_idx = (self.next_idx + 1) % self.parents.len();
            while let Some(cp) = queue.pop() {
                if collected_pieces.contains(&cp.number) {
                    piece = cp;
                    find = true;
                    break;
                }
            }
            if find {
                break;
            } else if self.next_idx == start_idx {
                info!("piece collector sleep");
                tokio::time::sleep(Duration::from_millis(500)).await;
            }
        }
        collected_pieces.remove(&piece.number);
        debug!("collect piece {} by default", piece.number);
        piece
    }

    fn register_parents(&self) {
        self.parent_selector
            .clone()
            .register_parents(self.task_id.clone(), &self.parents.clone());
    }

    pub fn unregister_parents(&self) {
        info!("[baowj] unregister");
        self.parent_selector
            .clone()
            .unregister_parents(self.task_id.clone());
    }

    pub fn finished(&self) -> bool {
        self.collected_pieces.is_empty()
    }
}

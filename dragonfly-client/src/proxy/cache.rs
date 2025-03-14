/*
 *     Copyright 2024 The Dragonfly Authors
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

use crate::resource::task::Task;
use dragonfly_api::common::v2::Range;
use dragonfly_api::dfdaemon::v2::DownloadTaskRequest;
use dragonfly_client_core::{Error, Result};
use dragonfly_client_util::http::{get_range, hashmap_to_headermap};
use lru::LruCache;
use std::cmp::{max, min};
use std::num::NonZeroUsize;
use std::sync::{Arc, Mutex};

/// Cache is the cache for storing http response by LRU algorithm.
#[derive(Clone)]
pub struct Cache {
    /// pieces stores the piece cache data with piece id and value.
    pieces: Arc<Mutex<LruCache<String, bytes::Bytes>>>,

    /// task is the task manager.
    task: Arc<Task>,
}

/// Cache implements the cache for storing http response by LRU algorithm.
impl Cache {
    /// new creates a new cache with the specified capacity.
    pub fn new(capacity: usize, task: Arc<Task>) -> Result<Self> {
        let capacity = NonZeroUsize::new(capacity).ok_or(Error::InvalidParameter)?;
        let pieces = Arc::new(Mutex::new(LruCache::new(capacity)));
        Ok(Cache { pieces, task })
    }

    /// get_by_request gets the content from the cache by the request.
    pub async fn get_by_request(
        &self,
        request: &DownloadTaskRequest,
    ) -> Result<Option<bytes::Bytes>> {
        let Some(download) = &request.download else {
            return Err(Error::InvalidParameter);
        };

        let task_id = self.task.id_generator.task_id(
            &download.url,
            download.piece_length,
            download.tag.as_deref(),
            download.application.as_deref(),
            download.filtered_query_params.clone(),
        )?;

        let Some(task) = self.task.get(&task_id)? else {
            return Ok(None);
        };

        let (Some(content_length), Some(piece_length)) =
            (task.content_length(), task.piece_length())
        else {
            return Ok(None);
        };

        let Ok(request_header) = hashmap_to_headermap(&download.request_header) else {
            return Ok(None);
        };

        let Ok(range) = get_range(&request_header, content_length) else {
            return Ok(None);
        };

        let interested_pieces =
            self.task
                .piece
                .calculate_interested(piece_length, content_length, range)?;

        // Calculate the content capacity based on the interested pieces and push the content into
        // the bytes.
        let content_capacity = interested_pieces.len() * piece_length as usize;
        let mut content = bytes::BytesMut::with_capacity(content_capacity);
        for interested_piece in interested_pieces {
            let piece_id = self.task.piece.id(&task_id, interested_piece.number);
            let Some(piece_content) = self.get_piece(&piece_id) else {
                return Ok(None);
            };

            // Calculate the target offset and length based on the range.
            let (piece_target_offset, piece_target_length) =
                calculate_piece_range(interested_piece.offset, interested_piece.length, range);

            let begin = piece_target_offset;
            let end = piece_target_offset + piece_target_length;
            if begin >= piece_content.len() || end > piece_content.len() {
                return Err(Error::InvalidParameter);
            }

            let piece_content = piece_content.slice(begin..end);
            content.extend_from_slice(&piece_content);
        }

        Ok(Some(content.freeze()))
    }

    /// get_piece gets the piece content from the cache.
    pub fn get_piece(&self, id: &str) -> Option<bytes::Bytes> {
        let mut pieces = self.pieces.lock().unwrap();
        pieces.get(id).cloned()
    }

    /// add_piece create the piece content into the cache, if the key already exists, no operation will
    /// be performed.
    pub fn add_piece(&self, id: &str, content: bytes::Bytes) {
        let mut pieces = self.pieces.lock().unwrap();
        if pieces.contains(id) {
            return;
        }

        pieces.push(id.to_string(), content);
    }

    /// contains_piece checks whether the piece exists in the cache.
    pub fn contains_piece(&self, id: &str) -> bool {
        let pieces = self.pieces.lock().unwrap();
        pieces.contains(id)
    }
}

/// calculate_piece_range calculates the target offset and length based on the piece range and
/// request range.
pub fn calculate_piece_range(offset: u64, length: u64, range: Option<Range>) -> (usize, usize) {
    if let Some(range) = range {
        let target_offset = max(offset, range.start) - offset;
        let target_length =
            min(offset + length - 1, range.start + range.length - 1) - target_offset - offset + 1;

        (target_offset as usize, target_length as usize)
    } else {
        (0, length as usize)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_calculate_piece_range() {
        let test_cases = vec![
            (1, 4, None, 0, 4),
            (
                1,
                4,
                Some(Range {
                    start: 1,
                    length: 4,
                }),
                0,
                4,
            ),
            (
                1,
                4,
                Some(Range {
                    start: 2,
                    length: 1,
                }),
                1,
                1,
            ),
            (
                1,
                4,
                Some(Range {
                    start: 1,
                    length: 1,
                }),
                0,
                1,
            ),
            (
                1,
                4,
                Some(Range {
                    start: 4,
                    length: 1,
                }),
                3,
                1,
            ),
            (
                1,
                4,
                Some(Range {
                    start: 0,
                    length: 2,
                }),
                0,
                1,
            ),
            (
                1,
                4,
                Some(Range {
                    start: 4,
                    length: 3,
                }),
                3,
                1,
            ),
        ];

        for (piece_offset, piece_length, range, expected_offset, expected_length) in test_cases {
            let (target_offset, target_length) =
                calculate_piece_range(piece_offset, piece_length, range);
            assert_eq!(target_offset, expected_offset);
            assert_eq!(target_length, expected_length);
        }
    }
}

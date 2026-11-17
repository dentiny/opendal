// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

use std::sync::{Arc, Mutex};
use std::time::Duration;

use backon::{BackoffBuilder, ExponentialBuilder};
use opendal_core::{Error, raw::Operation};

use crate::{RetryEvent, RetryInterceptor};

pub(super) fn prepare<I: RetryInterceptor>(
    builder: ExponentialBuilder,
    interceptor: Arc<I>,
    operation: Operation,
) -> (impl backon::Backoff, impl FnMut(&Error) -> bool) {
    let pending = Arc::new(Mutex::new(None));
    let selected = pending.clone();
    let mut delays = builder.build();
    let mut attempt = 0;
    let retryable = move |error: &Error| {
        if !error.is_temporary() {
            return false;
        }
        let Some(delay) = delays.next() else {
            return false;
        };
        attempt += 1;
        let delay = interceptor.retry_delay(RetryEvent {
            op: operation,
            err: error,
            retry_after: delay,
            attempt,
        });
        *selected.lock().expect("retry delay lock poisoned") = delay;
        delay.is_some()
    };
    (SelectedDelay(pending), retryable)
}

// Backon checks retryability before advancing backoff. Sharing one selected
// delay connects the interceptor to both regular and context-owning retries.
struct SelectedDelay(Arc<Mutex<Option<Duration>>>);

impl Iterator for SelectedDelay {
    type Item = Duration;

    fn next(&mut self) -> Option<Duration> {
        self.0.lock().expect("retry delay lock poisoned").take()
    }
}

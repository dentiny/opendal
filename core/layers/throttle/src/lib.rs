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

#![cfg_attr(docsrs, feature(doc_cfg))]
#![deny(missing_docs)]

use std::num::NonZeroU32;
use std::sync::Arc;

use governor::Quota;
use governor::RateLimiter;
use governor::clock::DefaultClock;
use governor::middleware::NoOpMiddleware;
use governor::state::InMemoryState;
use governor::state::NotKeyed;
use opendal_core::raw::*;
use opendal_core::*;
use opendal_layer_throttle_base as base;

/// Share an atomic [`RateLimiter`] instance across all threads in one operator.
type SharedRateLimiter =
    Arc<RateLimiter<NotKeyed, InMemoryState, DefaultClock, NoOpMiddleware>>;

/// Newtype around a `governor` [`RateLimiter`] so we can implement the
/// [`base::ThrottleRateLimiter`] trait without violating the orphan rule.
#[derive(Clone)]
pub struct GovernorRateLimiter(SharedRateLimiter);

impl base::ThrottleRateLimiter for GovernorRateLimiter {
    async fn until_n_ready(&self, n: NonZeroU32) -> Result<()> {
        self.0.as_ref().until_n_ready(n).await.map_err(|_| {
            Error::new(
                ErrorKind::RateLimited,
                "burst size is smaller than the request size",
            )
        })
    }
}

/// Add a bandwidth rate limiter to the underlying services.
///
/// # Throttle
///
/// There are several algorithms when it comes to rate limiting techniques.
/// This throttle layer uses the Generic Cell Rate Algorithm (GCRA) provided by
/// [Governor](https://docs.rs/governor/latest/governor/index.html).
/// By setting the `bandwidth` and `burst`, we can control the byte flow rate of underlying services.
///
/// # Note
///
/// When setting the ThrottleLayer, always consider the largest possible operation size as the burst size,
/// as **the burst size should be larger than any possible byte length to allow it to pass through**.
///
/// Read more about [Quota](https://docs.rs/governor/latest/governor/struct.Quota.html#examples).
///
/// # Examples
///
/// This example limits bandwidth to 10 KiB/s and burst size to 10 MiB.
///
/// ```no_run
/// # use opendal_core::services;
/// # use opendal_core::Operator;
/// # use opendal_core::Result;
/// # use opendal_layer_throttle::ThrottleLayer;
/// #
/// # fn main() -> Result<()> {
/// let _ = Operator::new(services::Memory::default())
///     .expect("must init")
///     .layer(ThrottleLayer::new(10 * 1024, 10000 * 1024))
///     .finish();
/// # Ok(())
/// # }
/// ```
#[derive(Clone)]
pub struct ThrottleLayer {
    rate_limiter: GovernorRateLimiter,
}

impl ThrottleLayer {
    /// Create a new `ThrottleLayer` with given bandwidth and burst.
    ///
    /// - bandwidth: the maximum number of bytes allowed to pass through per second.
    /// - burst: the maximum number of bytes allowed to pass through at once.
    pub fn new(bandwidth: u32, burst: u32) -> Self {
        assert!(bandwidth > 0);
        assert!(burst > 0);
        let bandwidth = NonZeroU32::new(bandwidth).unwrap();
        let burst = NonZeroU32::new(burst).unwrap();
        let rate_limiter = GovernorRateLimiter(Arc::new(RateLimiter::direct(
            Quota::per_second(bandwidth).allow_burst(burst),
        )));
        Self { rate_limiter }
    }
}

impl<A: Access> Layer<A> for ThrottleLayer {
    type LayeredAccess = base::ThrottleAccessor<A, GovernorRateLimiter>;

    fn layer(&self, inner: A) -> Self::LayeredAccess {
        base::ThrottleLayer::new(self.rate_limiter.clone()).layer(inner)
    }
}

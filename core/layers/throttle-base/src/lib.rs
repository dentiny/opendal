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

//! OpenDAL throttle layer base components.
//!
//! This crate provides the trait and generic plumbing required to build a
//! throttle layer on top of an arbitrary rate-limit primitive. Concrete
//! implementations live in their own crates, e.g.
//! [`opendal-layer-throttle`](https://docs.rs/opendal-layer-throttle/) which
//! is built on top of [`governor`](https://docs.rs/governor/).
//!
//! Users who want to provide a custom rate limiter (for example one that
//! exposes additional metrics such as queue depth or blocking wait time)
//! should implement [`ThrottleRateLimiter`] for their own type and construct
//! a [`ThrottleLayer`] with it directly.

#![cfg_attr(docsrs, feature(doc_cfg))]
#![deny(missing_docs)]

use std::future::Future;
use std::num::NonZeroU32;

use opendal_core::raw::*;
use opendal_core::*;

/// Abstracts a rate-limit primitive used by [`ThrottleLayer`].
///
/// Implementors must be cheap to clone, since each created reader/writer
/// holds its own clone of the limiter.
pub trait ThrottleRateLimiter: Send + Sync + Clone + Unpin + 'static {
    /// Block until `n` units of capacity are available.
    ///
    /// Returns an error when the request can never be satisfied, for
    /// example when `n` exceeds the limiter's burst/capacity.
    fn until_n_ready(&self, n: NonZeroU32) -> impl Future<Output = Result<()>> + MaybeSend;
}

/// Add a bandwidth rate limiter to the underlying services.
///
/// This generic layer is the building block used by concrete throttle
/// layers. Most users should reach for the `governor`-based
/// `opendal_layer_throttle::ThrottleLayer` instead; use this layer directly
/// only when plugging in a custom [`ThrottleRateLimiter`].
#[derive(Clone)]
pub struct ThrottleLayer<L: ThrottleRateLimiter> {
    rate_limiter: L,
}

impl<L: ThrottleRateLimiter> ThrottleLayer<L> {
    /// Create a layer with any [`ThrottleRateLimiter`] implementation.
    pub fn new(rate_limiter: L) -> Self {
        Self { rate_limiter }
    }
}

impl<A: Access, L: ThrottleRateLimiter> Layer<A> for ThrottleLayer<L> {
    type LayeredAccess = ThrottleAccessor<A, L>;

    fn layer(&self, inner: A) -> Self::LayeredAccess {
        ThrottleAccessor {
            inner,
            rate_limiter: self.rate_limiter.clone(),
        }
    }
}

#[doc(hidden)]
pub struct ThrottleAccessor<A: Access, L: ThrottleRateLimiter> {
    inner: A,
    rate_limiter: L,
}

impl<A: Access, L: ThrottleRateLimiter> std::fmt::Debug for ThrottleAccessor<A, L> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ThrottleAccessor")
            .field("inner", &self.inner)
            .finish_non_exhaustive()
    }
}

impl<A: Access, L: ThrottleRateLimiter> LayeredAccess for ThrottleAccessor<A, L> {
    type Inner = A;
    type Reader = ThrottleWrapper<A::Reader, L>;
    type Writer = ThrottleWrapper<A::Writer, L>;
    type Lister = A::Lister;
    type Deleter = A::Deleter;

    fn inner(&self) -> &Self::Inner {
        &self.inner
    }

    async fn read(&self, path: &str, args: OpRead) -> Result<(RpRead, Self::Reader)> {
        let limiter = self.rate_limiter.clone();

        self.inner
            .read(path, args)
            .await
            .map(|(rp, r)| (rp, ThrottleWrapper::new(r, limiter)))
    }

    async fn write(&self, path: &str, args: OpWrite) -> Result<(RpWrite, Self::Writer)> {
        let limiter = self.rate_limiter.clone();

        self.inner
            .write(path, args)
            .await
            .map(|(rp, w)| (rp, ThrottleWrapper::new(w, limiter)))
    }

    async fn delete(&self) -> Result<(RpDelete, Self::Deleter)> {
        self.inner.delete().await
    }

    async fn list(&self, path: &str, args: OpList) -> Result<(RpList, Self::Lister)> {
        self.inner.list(path, args).await
    }
}

#[doc(hidden)]
pub struct ThrottleWrapper<R, L> {
    inner: R,
    limiter: L,
}

impl<R, L> ThrottleWrapper<R, L> {
    fn new(inner: R, limiter: L) -> Self {
        Self { inner, limiter }
    }
}

impl<R: oio::Read, L: ThrottleRateLimiter> oio::Read for ThrottleWrapper<R, L> {
    async fn read(&mut self) -> Result<Buffer> {
        self.inner.read().await
    }
}

impl<R: oio::Write, L: ThrottleRateLimiter> oio::Write for ThrottleWrapper<R, L> {
    async fn write(&mut self, bs: Buffer) -> Result<()> {
        let len = bs.len();
        if len == 0 {
            return self.inner.write(bs).await;
        }

        if len > u32::MAX as usize {
            return Err(Error::new(
                ErrorKind::RateLimited,
                "request size exceeds throttle quota capacity",
            ));
        }

        let buf_length =
            NonZeroU32::new(len as u32).expect("len is non-zero so NonZeroU32 must exist");

        self.limiter.until_n_ready(buf_length).await?;

        self.inner.write(bs).await
    }

    async fn abort(&mut self) -> Result<()> {
        self.inner.abort().await
    }

    async fn close(&mut self) -> Result<Metadata> {
        self.inner.close().await
    }
}

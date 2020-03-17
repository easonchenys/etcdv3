//! Etcd Lease RPC.

use crate::error::Result;
use crate::rpc::pb::etcdserverpb::lease_client::LeaseClient as PbLeaseClient;
use crate::rpc::pb::etcdserverpb::{
    LeaseGrantRequest as PbLeaseGrantRequest,
    LeaseGrantResponse as PbLeaseGrantResponse,
};

use crate::rpc::{get_prefix, KeyValue, ResponseHeader};
use tonic::transport::Channel;
use tonic::{Interceptor, IntoRequest, Request};

/// Client for lease operations.
#[repr(transparent)]
pub struct LeaseClient {
    inner: PbLeaseClient<Channel>,
}

impl LeaseClient {
    /// Creates a lease client.
    #[inline]
    pub fn new(channel: Channel, interceptor: Option<Interceptor>) -> Self {
        let inner = match interceptor {
            Some(it) => PbLeaseClient::with_interceptor(channel, it),
            None => PbLeaseClient::new(channel),
        };

        Self { inner }
    }

    /// lease_grant creates a lease which expires if the server does not receive a keepAlive
    /// within a given time to live period. All keys attached to the lease will be expired and
    /// deleted if the lease expires. Each expired key generates a delete event in the event history.
    #[inline]
    pub async fn lease_grant(
        &mut self,
        ttl: i64,
        id: i64,
        options: Option<LeaseGrantOptions>,
    ) -> Result<LeaseGrantResponse> {
        let resp = self
            .inner
            .lease_grant(options.unwrap_or_default().with_id(id).with_ttl(ttl))
            .await?
            .into_inner();
        Ok(LeaseGrantResponse::new(resp))
    }

    /// lease_revoke revokes a lease. All keys attached to the lease will expire and be deleted.
    #[inline]
    pub async fn lease_revoke(
        &mut self,
        id: i64,
        options: Option<LeaseRevokeOptions>,
    ) -> Result<LeaseRevokeResponse> {
        let resp = self
            .inner
            .lease_revoke(options.unwrap_or_default())
            .await?
            .into_inner();
        Ok(LeaseRevokeResponse::new(resp))
    }

    /// lease_keep_alive keeps the lease alive by streaming keep alive requests from the client
    /// to the server and streaming keep alive responses from the server to the client.
    #[inline]
    pub async fn lease_keep_alive(
        &mut self,
        id: i64,
        options: Option<LeaseKeepAliveOptions>,
    ) -> Result<LeaseKeepAliveResponse> {
        let resp = self
            .inner
            .lease_keep_alive(options.unwrap_or_default())
            .await?
            .into_inner();
        Ok(LeaseKeepAliveResponse::new(resp))
    }

    ///lease_time_to_live retrieves lease information.
    pub async fn lease_time_to_live(
       &mut self,
       id: i64,
       keys: bool,
       options: Option<LeaseTimeToLiveOptions>,
    ) -> Result<LeaseTimeToLiveResponse> {
        let resp = self
            .inner
            .lease_time_to_live(options.unwrap_or_default())
            .await?
            .into_inner();
        Ok(LeaseTimeToLiveResponse::new(resp))
    }

     /// lease_leases lists all existing leases.
    pub async fn lease_leases(
       &mut self,
       options: Option<LeaseLeasesOptions>,
    ) -> Result<LeaseLeasesResponse> {
         let resp = self
             .inner
             .lease_time_to_live(options.unwrap_or_default())
             .await?
             .into_inner();
         Ok(LeaseLeasesResponse::new(resp))
     }
}

/// Options for `leaseGrant` operation.
#[derive(Debug, Default, Clone)]
#[repr(transparent)]
pub struct LeaseGrantOptions(PbLeaseGrantRequest);

impl LeaseGrantOptions {
    /// Set ttl
    #[inline]
    fn with_ttl(mut self, ttl: i64) -> Self {
        self.0.ttl = ttl;
        self
    }

    /// Set id
    fn with_id(mut self, id: i64) -> Self {
        self.0.id = id;
        self
    }

    /// Creates a `LeaseGrantOptions`.
    #[inline]
    pub const fn new() -> Self {
        Self(PbLeaseGrantRequest {
            ttl: 0,
            id: 0,
        })
    }
}

impl From<LeaseGrantOptions> for PbLeaseGrantRequest {
    #[inline]
    fn from(options: LeaseGrantOptions) -> Self {
        options.0
    }
}

impl IntoRequest<PbLeaseGrantRequest> for LeaseGrantOptions {
    #[inline]
    fn into_request(self) -> Request<PbLeaseGrantRequest> {
        Request::new(self.into())
    }
}

/// Response for `leaseGrant` operation.
#[derive(Debug, Clone)]
#[repr(transparent)]
pub struct LeaseGrantResponse(PbLeaseGrantResponse);

impl LeaseGrantResponse {
    /// Create a new `LeaseGrantResponse` from pb lease grant response.
    #[inline]
    const fn new(resp: PbLeaseGrantResponse) -> Self {
        Self(resp)
    }

    /// Get response header.
    #[inline]
    pub fn header(&self) -> Option<&ResponseHeader> {
        self.0.header.as_ref().map(From::from)
    }

    /// Takes the header out of the response, leaving a [`None`] in its place.
    #[inline]
    pub fn take_header(&mut self) -> Option<ResponseHeader> {
        self.0.header.take().map(ResponseHeader::new)
    }

    /// TTL is the server chosen lease time-to-live in seconds
    #[inline]
    pub const fn ttl(&self) -> i64 {
        self.0.ttl
    }

    /// ID is the lease ID for the granted lease.
    #[inline]
    pub const fn id(&self) -> i64 { self.0.id }

    /// error message if return error.
    #[inline]
    pub fn error(&self) -> &[u8] { self.0.error.as_ref() }
}

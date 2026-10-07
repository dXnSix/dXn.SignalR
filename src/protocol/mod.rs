// Decoding and transport code must not panic on peer input; tests may.
#![cfg_attr(
    not(test),
    deny(
        clippy::unwrap_used,
        clippy::expect_used,
        clippy::panic,
        clippy::indexing_slicing
    )
)]

pub(crate) mod codec;
pub mod hub_protocol;
pub(crate) mod invoke;
pub(crate) mod messages;
#[cfg(feature = "messagepack")]
pub(crate) mod msgpack;
pub(crate) mod negotiate;
pub(crate) mod stateful;

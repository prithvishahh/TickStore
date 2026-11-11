//! TickStore is a columnar on-disk engine for financial tick data.
//!
//! Trades and quotes are partitioned by symbol into immutable chunks. Each
//! column inside a chunk is stored with the codec that is smallest for that
//! block (within a small band, the faster decoder wins). Queries memory-map
//! the chunks that survive a min/max timestamp prune and aggregate without
//! building a row for every print.
//!
//! Prices are integer ticks. `price_scale` on the store converts them to
//! currency units: `price_ticks / price_scale`.

pub mod bench;
pub mod catalog;
pub mod codec;
pub mod dataset_io;
pub mod error;
pub mod format;
pub mod generate;
pub mod reference;
pub mod rng;
pub mod store;
pub mod types;

mod cursor;

pub use error::{Error, Result};
pub use generate::{generate, Dataset, GenConfig, SymbolSeries};
pub use store::{Options, StoreStats, TickStore};
pub use types::{
    format_ticks, parse_interval, parse_ticks, AsofRow, Bar, Quote, Side, Trade, Vwap,
};

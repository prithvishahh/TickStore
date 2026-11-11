use thiserror::Error;

/// Errors returned by the storage engine, codecs, and loaders.
#[derive(Debug, Error)]
pub enum Error {
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),

    #[error("corrupt data: {0}")]
    Corrupt(String),

    #[error("{0}")]
    BadArgument(String),

    #[error("symbol not found: {0}")]
    NotFound(String),

    #[error("unsorted append for {symbol}: timestamp {ts} precedes {prev}")]
    Unsorted { symbol: String, prev: i64, ts: i64 },

    #[error("parse error at line {line}: {message}")]
    Parse { line: usize, message: String },

    #[error("json error: {0}")]
    Json(#[from] serde_json::Error),

    #[error("numeric overflow: {0}")]
    Overflow(String),
}

/// Crate-wide result type.
pub type Result<T> = std::result::Result<T, Error>;

impl Error {
    pub(crate) fn corrupt(msg: impl Into<String>) -> Self {
        Error::Corrupt(msg.into())
    }

    pub(crate) fn bad(msg: impl Into<String>) -> Self {
        Error::BadArgument(msg.into())
    }
}

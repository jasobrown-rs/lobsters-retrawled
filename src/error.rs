use thiserror::Error;

#[derive(Debug, Error)]
pub enum Error {
    #[error(transparent)]
    MySqlError(#[from] mysql_async::Error),
    #[error(transparent)]
    PostgresError(#[from] tokio_postgres::Error),
    #[error(transparent)]
    PostgresDpError(#[from] deadpool_postgres::tokio_postgres::Error),

    // #[error(transparent)]
    // PostgresDpPoolError(#[from] deadpool_postgres::PoolError),
    #[error("{0}")]
    PostgresPoolError(String),
    #[error(transparent)]
    PostgresPoolBuildError(#[from] deadpool_postgres::BuildError),
}

//
impl From<deadpool_postgres::PoolError> for Error {
    fn from(e: deadpool_postgres::PoolError) -> Self {
        let s = format!("{}", e);
        Error::PostgresPoolError(s)
    }
}

pub type Result<T, E = Error> = core::result::Result<T, E>;

#![feature(type_alias_impl_trait, impl_trait_in_assoc_type)]

use clap::{Parser, ValueEnum};
use std::time;

mod mysql;
mod postgresql;

#[derive(Clone, Copy, Eq, PartialEq, Debug, ValueEnum)]
enum Database {
    MySQL,
    PostgreSQL,
}

#[derive(Clone, Copy, Eq, PartialEq, Debug, ValueEnum)]
enum Variant {
    Original,
    Noria,
    Natural,
}

#[derive(Debug, Parser)]
pub(crate) struct Options {
    /// Reuest load scale factor for workload
    #[arg(long, default_value = "1.0")]
    scale: f64,

    /// Number of allowed concurrent requests
    #[arg(long, default_value = "50")]
    in_flight: usize,

    /// Set if the backend must be primed with initial stories and comments.
    #[arg(long, default_value = "false")]
    prime: bool,

    /// Which set of queries to run
    #[arg(long, default_value = "original")]
    queries: Variant,

    /// Benchmark runtime in seconds
    #[arg(short = 'r', long, default_value = "30")]
    runtime: u64,

    /// Use file-based serialized HdrHistograms. There are multiple histograms,
    /// two for each lobsters request.
    #[arg(long)]
    histogram: Option<String>,

    /// Database type
    #[arg(long, required = true)]
    database: Database,

    /// Database name (address)
    #[arg(long, default_value = "mysql://lobsters@localhost/soup")]
    dbn: String,
}

fn main() -> Result<(), Error> {
    let options = Options::parse();

    let mut wl = trawler::WorkloadBuilder::default();
    wl.scale(options.scale)
        .time(time::Duration::from_secs(options.runtime))
        .in_flight(options.in_flight);

    if let Some(ref h) = options.histogram {
        wl.with_histogram(h.as_str());
    }

    let s = match options.database {
        Database::MySQL => mysql::MysqlTrawlerBuilder::build(&options)?,
        Database::PostgreSQL => postgresql::PostgreSqlTrawlerBuilder::build(&options)?,
    };

    wl.run(s, options.prime);
    Ok(())
}

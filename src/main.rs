// #![feature(type_alias_impl_trait, impl_trait_in_assoc_type)]

extern crate mysql_async as my;

use anyhow::Result;
use async_trait::async_trait;
use clap::{Parser, ValueEnum};
use metrics::{histogram, Histogram};
use metrics_exporter_prometheus::PrometheusBuilder;
use mysql_async::prelude::*;
use mysql_async::{Conn, Opts, OptsBuilder, Pool, PoolConstraints, PoolOpts, Row};

use std::collections::HashMap;
use std::mem;
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::time::{Duration, Instant};
use trawler::{LobstersRequest, RequestProcessor, TrawlerRequest};

const ORIGINAL_SCHEMA: &str = include_str!("db-schema/original.sql");
const NORIA_SCHEMA: &str = include_str!("db-schema/noria.sql");
// const NATURAL_SCHEMA: &str = include_str!("db-schema/natural.sql");

const PUSH_GATEWAY_PUSH_INTERVAL: Duration = Duration::from_secs(5);

mod endpoints;

#[derive(Clone, Copy, Eq, PartialEq, Debug, ValueEnum)]
enum Variant {
    Original,
    Noria,
    //    Natural,
}

#[derive(Clone)]
struct MysqlTrawler {
    opts: OptsBuilder,
    pool: Option<Pool>,
    variant: Variant,
    pages_histos: HashMap<String, Histogram>,
}

impl MysqlTrawler {
    fn new(options: Options) -> Result<Self> {
        // check that we can indeed connect
        let opts = OptsBuilder::from_opts(Opts::from_url(options.dbn.as_str())?)
            .tcp_nodelay(true)
            .pool_opts(
                // Note: version 0.23.0 of the mysql-async driver did not automatically
                // reset the connection when it went back into the pool. version 0.33.0
                // does indeed reset. Explicitly disabling it (for now, at least :shrug:)
                PoolOpts::default()
                    .with_constraints(
                        PoolConstraints::new(options.in_flight, options.in_flight).unwrap(),
                    )
                    .with_reset_connection(false),
            );
        let pool = Pool::new(opts.clone());
        Ok(Self {
            opts,
            pool: Some(pool),
            variant: options.queries,
            pages_histos: Default::default(),
        })
    }

    fn record_histo(&mut self, page_name: String, elaped: Duration) {
        let histo = self
            .pages_histos
            .entry(page_name.clone())
            .or_insert_with(|| {
                let labels = vec![("page", page_name)];
                histogram!("lobsters_page", &labels)
            })
            .clone();
        histo.record(elaped);
    }
}

#[async_trait]
impl RequestProcessor for MysqlTrawler {
    async fn data_prime_init(&mut self) -> Result<()> {
        // we need a special conn for setup
        let opts: OptsBuilder = self
            .opts
            .clone()
            .pool_opts(PoolOpts::default().with_constraints(PoolConstraints::new(1, 1).unwrap()))
            .db_name(None::<String>)
            .prefer_socket(false);

        let db: String = Opts::from(self.opts.clone()).db_name().unwrap().to_string();
        let db_drop = format!("DROP DATABASE IF EXISTS {}", db);
        let db_create = format!("CREATE DATABASE {}", db);
        let db_use = format!("USE {}", db);
        let mut c = Conn::new(opts).await?;
        c.query_drop(&db_drop).await?;
        c.query_drop(&db_create).await?;
        c.query_drop(&db_use).await?;
        let schema = match self.variant {
            Variant::Original => ORIGINAL_SCHEMA,
            Variant::Noria => NORIA_SCHEMA,
            //                    Variant::Natural => NATURAL_SCHEMA,
        };
        let mut current_q = String::new();
        for line in schema.lines() {
            if line.starts_with("--") || line.is_empty() {
                continue;
            }
            if !current_q.is_empty() {
                current_q.push(' ');
            }
            current_q.push_str(line);
            if current_q.ends_with(';') {
                c.query_drop(&current_q).await?;
                current_q.clear();
            }
        }

        Ok(())
    }

    async fn process(
        &mut self,
        TrawlerRequest {
            user: acting_as,
            page: req,
            is_priming: priming,
            ..
        }: TrawlerRequest,
    ) -> Result<()> {
        if self.pool.is_none() {
            // a closed pool was acceptable under earlier iterations of this app ...
            return Ok(());
        }
        let c = self.pool.as_mut().expect("asdf").get_conn(); // just checked

        // really?!? how can it be this hard to get a name from the page enum?
        let page_name = LobstersRequest::variant_name(&mem::discriminant(&req)).to_string();

        macro_rules! handle_req {
            ($module:tt, $req:expr) => {{
                match req {
                    LobstersRequest::User(uid) => {
                        endpoints::$module::user::handle(c, acting_as, uid).await
                    }
                    LobstersRequest::Frontpage => {
                        endpoints::$module::frontpage::handle(c, acting_as).await
                    }
                    LobstersRequest::Comments => {
                        endpoints::$module::comments::handle(c, acting_as).await
                    }
                    LobstersRequest::Recent => {
                        endpoints::$module::recent::handle(c, acting_as).await
                    }
                    LobstersRequest::Login => {
                        let mut c = c.await?;
                        let user = c
                            .exec_first::<Row, _, _>(
                                "SELECT 1 as one FROM `users` WHERE `users`.`username` = ?",
                                (format!("user{}", acting_as.unwrap()),),
                            )
                            .await?;

                        if user.is_none() {
                            let uid = acting_as.unwrap();
                            c.exec_drop(
                                "INSERT INTO `users` (`username`) VALUES (?)",
                                (format!("user{}", uid),),
                            )
                            .await?;
                        }

                        Ok((c, false))
                    }
                    LobstersRequest::Logout => Ok((c.await?, false)),
                    LobstersRequest::Story(id) => {
                        endpoints::$module::story::handle(c, acting_as, id).await
                    }
                    LobstersRequest::StoryVote(story, v) => {
                        endpoints::$module::story_vote::handle(c, acting_as, story, v).await
                    }
                    LobstersRequest::CommentVote(comment, v) => {
                        endpoints::$module::comment_vote::handle(c, acting_as, comment, v).await
                    }
                    LobstersRequest::Submit { id, title } => {
                        endpoints::$module::submit::handle(c, acting_as, id, title, priming).await
                    }
                    LobstersRequest::Comment { id, story, parent } => {
                        endpoints::$module::comment::handle(
                            c, acting_as, id, story, parent, priming,
                        )
                        .await
                    }
                }
            }};
        }

        let timer = Instant::now();
        let variant = self.variant;

        let (c, with_notifications) = match variant {
            Variant::Original => handle_req!(original, req),
            Variant::Noria => handle_req!(noria, req),
            // Variant::Natural => handle_req!(natural, req),
        }?;

        // notifications
        if let Some(uid) = acting_as {
            if with_notifications && !priming {
                match variant {
                    Variant::Original => endpoints::original::notifications(c, uid).await,
                    Variant::Noria => endpoints::noria::notifications(c, uid).await,
                    // Variant::Natural => endpoints::natural::notifications(c, uid).await,
                }?;
            }
        };

        self.record_histo(page_name, timer.elapsed());
        Ok(())
    }

    async fn shutdown(&mut self) -> Result<()> {
        if let Some(pool) = self.pool.take() {
            pool.disconnect().await?
        }
        Ok(())
    }
}

#[derive(Clone, Debug, Parser)]
struct Options {
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

    /// Database name (address)
    #[arg(long, default_value = "mysql://lobsters@localhost/soup")]
    dbn: String,

    /// Enable reporting of metrics to prometheus.
    ///
    /// Note: defaulting to `true` basically makes this always true.
    #[arg(long, default_value = "true")]
    prometheus_metrics: bool,

    /// Enable reporting of metrics to prometheus.
    ///
    /// Note: defaulting to `true` basically makes this always true.
    #[arg(long)]
    prometheus_push_gateway: Option<String>,
}

fn init_prometheus(options: &Options) {
    // The scrape port for Prometheus - i.e., you want to load it in a browser.
    // One port higher than what we use, by default, for
    // in case you happen to run this benchmark on the same machine.
    let scrape_port = 6035;
    let socket = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(0, 0, 0, 0)), scrape_port);
    println!("init: setting prometheus scrape endpoint to: {:?}", &socket);
    let mut builder = PrometheusBuilder::new().with_http_listener(socket);

    // Note: promethus can either host the scrape endpoint or push to the gateway,
    // but not both.
    if let Some(ref addr) = options.prometheus_push_gateway {
        println!(
            "init: send prometheus metrics to push gateway at: {}",
            &addr
        );
        builder = builder
            .with_push_gateway(addr, PUSH_GATEWAY_PUSH_INTERVAL, None, None)
            .expect("failed to add push gateway");
    }

    builder
        .install()
        .expect("failed to install prometheus recorder/exporter");
}

/// A best-effort attempt to clean up metrics reporting. Normally, once this process exits,
/// the last reported value for a metric that was pushed to the gateway will, essentially, remain
/// that value until overwritten at a future execution. This sounds fine, but unforunately when
/// graphing that data via grafana, the latency data values in the graphs will continue at the
/// last recorded value forever (super annoying as it looks like a straight, unwaving line
/// in perpetuity, even after the benchmarks is waaay done).
///
/// There's two solutions:
/// 1. after the load test is done, sleep the main thread for 60 seconds. This will allow the
/// windowed data in the histogram to clear out, and get reported by the background thread
/// as record a value of 0 to the gateway.
/// 2. after the load test is done, naively call the gateway to DELETE all the metrics for the job.
/// As long as those values have been reported _at some point_ to prometheus, you have some workable data.
///
/// This implementation opts for the first, as it's easier to code as less jarring to prometheus, as the
/// values are zero, not none - so it can always have a value to graph (avoids the "no data found" confusion).
fn stop_prometheus() {
    // this could probably be shorted, i didn't try that hard :shrug:
    let sleep_time =
        Duration::from_secs(60) + PUSH_GATEWAY_PUSH_INTERVAL + PUSH_GATEWAY_PUSH_INTERVAL;
    println!(
        "shutdown: waiting {} seconds until metrics gateway clears",
        &sleep_time.as_secs()
    );
    std::thread::sleep(sleep_time);
}

fn main() -> Result<()> {
    let options = Options::parse();
    println!("launching lobsters benchmark, options: {:?}", &options);

    if options.prometheus_metrics {
        init_prometheus(&options);
    }

    let mut wl = trawler::WorkloadBuilder::default();
    wl.scale(options.scale)
        .time(Duration::from_secs(options.runtime))
        .in_flight(options.in_flight);

    if let Some(ref h) = options.histogram {
        wl.with_histogram(h.clone());
    }

    let mysql_trawler = MysqlTrawler::new(options.clone())?;
    wl.run(mysql_trawler, options.prime);

    if options.prometheus_metrics {
        stop_prometheus();
    }

    Ok(())
}

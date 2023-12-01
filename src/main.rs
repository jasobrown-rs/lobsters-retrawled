#![feature(type_alias_impl_trait, impl_trait_in_assoc_type)]

extern crate mysql_async as my;

use clap::{Parser, ValueEnum};
use mysql_async::prelude::*;
use mysql_async::{Conn, Error, Opts, OptsBuilder, Pool, PoolConstraints, PoolOpts, Row};
use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};
use std::time;
use tower_service::Service;
use trawler::{LobstersRequest, TrawlerRequest};

const ORIGINAL_SCHEMA: &str = include_str!("db-schema/original.sql");
const NORIA_SCHEMA: &str = include_str!("db-schema/noria.sql");
const NATURAL_SCHEMA: &str = include_str!("db-schema/natural.sql");

#[derive(Clone, Copy, Eq, PartialEq, Debug, ValueEnum)]
enum Variant {
    Original,
    Noria,
    //    Natural,
}

struct MysqlTrawlerBuilder {
    opts: OptsBuilder,
    variant: Variant,
}

enum MaybeConn {
    None,
    Pending(my::futures::GetConn),
    Ready(Conn),
}

struct MysqlTrawler {
    c: Pool,
    next_conn: MaybeConn,
    variant: Variant,
}
/*
impl Drop for MysqlTrawler {
    fn drop(&mut self) {
        self.c.disconnect();
    }
}
*/

mod endpoints;

impl Service<bool> for MysqlTrawlerBuilder {
    type Response = MysqlTrawler;
    type Error = mysql_async::Error;
    type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send>>;
    fn poll_ready(&mut self, _: &mut Context) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }
    fn call(&mut self, priming: bool) -> Self::Future {
        let orig_opts = self.opts.clone();
        let variant = self.variant;

        if priming {
            // we need a special conn for setup
            let opts: OptsBuilder = self
                .opts
                .clone()
                .pool_opts(
                    PoolOpts::default().with_constraints(PoolConstraints::new(1, 1).unwrap()),
                )
                .db_name(None::<String>)
                .prefer_socket(false);

            let db: String = Opts::from(self.opts.clone()).db_name().unwrap().to_string();
            let db_drop = format!("DROP DATABASE IF EXISTS {}", db);
            let db_create = format!("CREATE DATABASE {}", db);
            let db_use = format!("USE {}", db);
            Box::pin(async move {
                let mut c = Conn::new(opts).await?;
                c.query_drop(&db_drop).await?;
                c.query_drop(&db_create).await?;
                c.query_drop(&db_use).await?;
                let schema = match variant {
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

                Ok(MysqlTrawler {
                    c: Pool::new(orig_opts.clone()),
                    next_conn: MaybeConn::None,
                    variant,
                })
            })
        } else {
            Box::pin(async move {
                Ok(MysqlTrawler {
                    c: Pool::new(orig_opts.clone()),
                    next_conn: MaybeConn::None,
                    variant,
                })
            })
        }
    }
}

impl Service<TrawlerRequest> for MysqlTrawler {
    type Response = ();
    type Error = mysql_async::Error;
    type Future = impl Future<Output = Result<Self::Response, Self::Error>> + Send;
    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        loop {
            match self.next_conn {
                MaybeConn::None => {
                    self.next_conn = MaybeConn::Pending(self.c.get_conn());
                }
                MaybeConn::Pending(ref mut getconn) => {
                    if let Poll::Ready(conn) = Pin::new(getconn).poll(cx)? {
                        self.next_conn = MaybeConn::Ready(conn);
                    } else {
                        return Poll::Pending;
                    }
                }
                MaybeConn::Ready(_) => {
                    return Poll::Ready(Ok(()));
                }
            }
        }
    }
    fn call(
        &mut self,
        TrawlerRequest {
            user: acting_as,
            page: req,
            is_priming: priming,
            ..
        }: TrawlerRequest,
    ) -> Self::Future {
        let c = match std::mem::replace(&mut self.next_conn, MaybeConn::None) {
            MaybeConn::None | MaybeConn::Pending(_) => {
                unreachable!("call called without poll_ready")
            }
            MaybeConn::Ready(c) => c,
        };
        let c = futures_util::future::ready(Ok(c));

        // TODO: traffic management
        // https://github.com/lobsters/lobsters/blob/master/app/controllers/application_controller.rb#L37
        /*
        let c = c.and_then(|c| {
            c.start_transaction(my::TransactionOptions::new())
                .and_then(|t| {
                    t.drop_query(
                        "SELECT keystores.* FROM keystores \
                         WHERE keystores.key = 'traffic:date' FOR UPDATE",
                    )
                })
                .and_then(|t| {
                    t.drop_query(
                        "SELECT keystores.* FROM keystores \
                         WHERE keystores.key = 'traffic:hits' FOR UPDATE",
                    )
                })
                .and_then(|t| {
                    t.drop_query(
                        "UPDATE keystores SET value = 100 \
                         WHERE keystores.key = 'traffic:hits'",
                    )
                })
                .and_then(|t| {
                    t.drop_query(
                        "UPDATE keystores SET value = 1521590012 \
                         WHERE keystores.key = 'traffic:date'",
                    )
                })
                .and_then(|t| t.commit())
        });
        */

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

        let variant = self.variant;
        Box::pin(async move {
            let inner = async move {
                let (c, with_notifications) = match variant {
                    Variant::Original => handle_req!(original, req),
                    Variant::Noria => handle_req!(noria, req),
                    //                    Variant::Natural => handle_req!(natural, req),
                }?;

                // notifications
                if let Some(uid) = acting_as {
                    if with_notifications && !priming {
                        match variant {
                            Variant::Original => endpoints::original::notifications(c, uid).await,
                            Variant::Noria => endpoints::noria::notifications(c, uid).await,
                            //                            Variant::Natural => endpoints::natural::notifications(c, uid).await,
                        }?;
                    }
                }

                Ok(())
            };

            // if the pool is disconnected, it just means that we exited while there were still
            // outstanding requests. that's fine.
            match inner.await {
                Ok(())
                | Err(mysql_async::Error::Driver(mysql_async::DriverError::PoolDisconnected)) => {
                    Ok(())
                }
                Err(e) => Err(e),
            }
        })
    }
}

impl trawler::AsyncShutdown for MysqlTrawler {
    type Future = impl Future<Output = ()>;
    fn shutdown(mut self) -> Self::Future {
        let _ = std::mem::replace(&mut self.next_conn, MaybeConn::None);
        async move {
            self.c.disconnect().await.unwrap();
        }
    }
}

#[derive(Debug, Parser)]
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
                .with_reset_connection(true),
        );
    let s = MysqlTrawlerBuilder {
        opts,
        variant: options.queries,
    };

    wl.run(s, options.prime);
    Ok(())
}

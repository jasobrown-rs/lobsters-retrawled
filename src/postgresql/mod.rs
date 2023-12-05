use std::future::Future;
use std::pin::Pin;
use std::str::FromStr;
use std::task::{Context, Poll};

use deadpool_postgres::tokio_postgres::{Config, NoTls};
use deadpool_postgres::{Client, Manager, ManagerConfig, Pool, RecyclingMethod};
use tokio_postgres::Row;
use tower_service::Service;
use trawler::{LobstersRequest, TrawlerRequest};

use crate::error::{Error, Result};
use crate::{Options, Variant};
mod endpoints;

const ORIGINAL_SCHEMA: &str = include_str!("db-schema/original.sql");
//const NORIA_SCHEMA: &str = include_str!("db-schema/noria.sql");
//const NATURAL_SCHEMA: &str = include_str!("db-schema/natural.sql");

pub struct PostgreSqlTrawlerBuilder {
    options: Options,
}

impl PostgreSqlTrawlerBuilder {
    pub fn new(options: Options) -> Self {
        Self { options }
    }

    fn build_pool(&self) -> Result<Pool, Error> {
        Self::build_pool_from_config(&self.options, None)
    }

    fn build_pool_from_config(options: &Options, db_name: Option<&str>) -> Result<Pool, Error> {
        let mut config = Config::from_str(&options.dbn)?;
        if let Some(override_name) = db_name {
            config.dbname(override_name);
        }
        let mgr_config = ManagerConfig {
            // NOTE: mysql_async pooling, by default, will clear out everything from
            // the connection (esp. prepared statements) when returning to pool.
            //`RecyclingMethod` looks  heavyweight than that, so YMMV.
            recycling_method: RecyclingMethod::Fast,
        };
        let mgr = Manager::from_config(config, NoTls, mgr_config);
        Ok(Pool::builder(mgr).max_size(options.in_flight).build()?)
    }
}

impl Service<bool> for PostgreSqlTrawlerBuilder {
    type Response = PostgreSqlTrawler;
    type Error = Error;
    type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send>>;

    fn poll_ready(&mut self, _: &mut Context) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, priming: bool) -> Self::Future {
        let variant = self.options.queries;
        let ret_pool = self.build_pool().unwrap();

        if priming {
            // we need a special conn for setup. the base MySQL code only used a
            // single connection for the priming, so naively replicating that here :shrug:
            let mut opts = self.options.clone();
            opts.in_flight = 1;
            let pool = Self::build_pool_from_config(&opts, Some("postgres")).unwrap();

            let db: String = Config::from_str(&self.options.dbn)
                .unwrap()
                .get_dbname()
                .expect("must have a database name defined")
                .to_string();

            let db_drop = format!("DROP DATABASE IF EXISTS {}", db);
            let db_create = format!("CREATE DATABASE {}", db);
            let db_use = format!("USE {}", db);
            Box::pin(async move {
                let c = pool.get().await?;
                c.simple_query(&db_drop).await?;
                c.simple_query(&db_create).await?;
                c.simple_query(&db_use).await?;
                let schema = match variant {
                    Variant::Original => ORIGINAL_SCHEMA,
                    _ => unimplemented!(),
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
                        c.simple_query(&current_q).await?;
                        current_q.clear();
                    }
                }

                Ok(PostgreSqlTrawler {
                    pool: ret_pool,
                    variant,
                })
            })
        } else {
            Box::pin(async move {
                Ok(PostgreSqlTrawler {
                    pool: ret_pool,
                    variant,
                })
            })
        }
    }
}

pub struct PostgreSqlTrawler {
    pool: Pool,
    variant: Variant,
}

impl Service<TrawlerRequest> for PostgreSqlTrawler {
    type Response = ();
    type Error = Error;
    type Future = impl Future<Output = Result<Self::Response, Self::Error>> + Send;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
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
        let variant = self.variant;
        let pool = self.pool.clone();

        Box::pin(async move {
            let inner = async move {
                macro_rules! handle_req {
                    ($module:tt, $req:expr) => {{
                        match req {
                            LobstersRequest::User(uid) => {
                                endpoints::$module::user::handle(pool, acting_as, uid).await
                            }
                            LobstersRequest::Frontpage => {
                                endpoints::$module::frontpage::handle(pool, acting_as).await
                            }
                            LobstersRequest::Comments => {
                                endpoints::$module::comments::handle(pool, acting_as).await
                            }
                            LobstersRequest::Recent => {
                                endpoints::$module::recent::handle(pool, acting_as).await
                            }
                            LobstersRequest::Login => {
                                let c = pool.get().await?;
                                let user = c
                                    .query_opt(
                                        "SELECT 1 as one FROM `users` WHERE `users`.`username` = $1",
                                        &[&format!("user{}", acting_as.unwrap())],
                                    )
                                    .await?;

                                if user.is_none() {
                                    let uid = acting_as.unwrap();
                                    c.execute(
                                        "INSERT INTO `users` (`username`) VALUES (?)",
                                        &[&format!("user{}", uid),],
                                    )
                                    .await?;
                                }

                                Ok((c, false))
                            }
                            LobstersRequest::Logout => Ok((pool.get().await?, false)),
                            LobstersRequest::Story(id) => {
                                endpoints::$module::story::handle(pool, acting_as, id).await
                            }
                            LobstersRequest::StoryVote(story, v) => {
                                endpoints::$module::story_vote::handle(pool, acting_as, story, v)
                                    .await
                            }
                            LobstersRequest::CommentVote(comment, v) => {
                                endpoints::$module::comment_vote::handle(
                                    pool, acting_as, comment, v,
                                )
                                .await
                            }
                            LobstersRequest::Submit { id, title } => {
                                endpoints::$module::submit::handle(
                                    pool, acting_as, id, title, priming,
                                )
                                .await
                            }
                            LobstersRequest::Comment { id, story, parent } => {
                                endpoints::$module::comment::handle(
                                    pool, acting_as, id, story, parent, priming,
                                )
                                .await
                            }
                        }
                    }};
                }

                let (c, with_notifications) = match variant {
                    Variant::Original => handle_req!(original, req),
                    _ => unimplemented!(),
                }?;

                // // notifications
                if let Some(uid) = acting_as {
                    if with_notifications && !priming {
                        let res = match variant {
                            Variant::Original => endpoints::original::notifications(c, uid).await,
                            _ => unimplemented!(),
                        }?;
                    }
                }

                Ok::<(), Error>(())
            };

            // if the pool is disconnected, it just means that we exited while there were still
            // outstanding requests. that's fine.
            match inner.await {
                Ok(())
                    // | Err(mysql_async::Error::Driver(mysql_async::DriverError::PoolDisconnected))
                    => {
                    Ok(())
                }
                Err(e) => panic!("asdfas"),
            }
        })
    }
}

impl trawler::AsyncShutdown for PostgreSqlTrawler {
    type Future = impl Future<Output = ()>;
    fn shutdown(self) -> Self::Future {
        async move {
            self.pool.close();
        }
    }
}

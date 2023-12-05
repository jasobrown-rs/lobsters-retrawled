use std::future::Future;

use deadpool_postgres::{Client, Pool};
use trawler::UserId;

use crate::error::{Error, Result};

pub(crate) async fn handle(
    pool: Pool,
    _acting_as: Option<UserId>,
    uid: UserId,
) -> Result<(Client, bool), Error> {
    let c = pool.get().await?;
    let user = c
        .query_opt(
            "SELECT  `users`.* FROM `users` \
             WHERE `users`.`username` = ?",
            &[&format!("user{}", uid)],
        )
        .await?;
    let uid = match user {
        Some(uid) => uid.get::<&str, u32>("id"),
        //        Some(uid) => uid.get::<&str, _>("id").unwrap(),
        None => {
            return Ok((c, false));
        }
    };

    // most popular tag
    c.execute(
        "SELECT  `tags`.* FROM `tags` \
             INNER JOIN `taggings` ON `taggings`.`tag_id` = `tags`.`id` \
             INNER JOIN `stories` ON `stories`.`id` = `taggings`.`story_id` \
             WHERE `tags`.`inactive` = 0 \
             AND `stories`.`user_id` = ? \
             GROUP BY `tags`.`id` \
             ORDER BY COUNT(*) desc LIMIT 1",
        &[&uid],
    )
    .await?;

    c.execute(
        "SELECT  `keystores`.* \
             FROM `keystores` \
             WHERE `keystores`.`key` = ?",
        &[&format!("user:{}:stories_submitted", uid)],
    )
    .await?;

    c.execute(
        "SELECT  `keystores`.* \
             FROM `keystores` \
             WHERE `keystores`.`key` = ?",
        &[&format!("user:{}:comments_posted", uid)],
    )
    .await?;

    c.execute(
        "SELECT  1 AS one FROM `hats` \
             WHERE `hats`.`user_id` = ? LIMIT 1",
        &[&uid],
    )
    .await?;

    Ok((c, true))
}

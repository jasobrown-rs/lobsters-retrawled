use mysql_async::prelude::*;
use mysql_async::{Error, Row};
use std::future::Future;
use trawler::UserId;

pub(crate) async fn handle<F>(
    c: F,
    _acting_as: Option<UserId>,
    uid: UserId,
) -> Result<(my::Conn, bool), Error>
where
    F: 'static + Future<Output = Result<my::Conn, Error>> + Send,
{
    let mut c = c.await?;
    let user = c
        .exec_first::<Row, _, _>(
            "SELECT  `users`.* FROM `users` \
             WHERE `users`.`username` = ?",
            (format!("user{}", uid),),
        )
        .await?;
    let uid = match user {
        Some(uid) => uid.get::<u32, _>("id").unwrap(),
        None => {
            return Ok((c, false));
        }
    };

    // most popular tag
    let mut rows = c
        .exec_iter(
            "SELECT  `tags`.`id`, COUNT(*) AS `count` FROM `taggings` \
             INNER JOIN `tags` ON `taggings`.`tag_id` = `tags`.`id` \
             INNER JOIN `stories` ON `stories`.`id` = `taggings`.`story_id` \
             WHERE `tags`.`inactive` = 0 \
             AND `stories`.`user_id` = ? \
             GROUP BY `tags`.`id` \
             ORDER BY `count` desc LIMIT 1",
            (uid,),
        )
        .await?
        .collect_and_drop::<my::Row>()
        .await?;

    if !rows.is_empty() {
        let tag = rows.swap_remove(0);
        c.exec_drop(
            "SELECT  `tags`.* \
                 FROM `tags` \
                 WHERE `tags`.`id` = ?",
            (tag.get::<u32, _>("id").unwrap(),),
        )
        .await?;
    }

    c.exec_drop(
        "SELECT  `keystores`.* \
             FROM `keystores` \
             WHERE `keystores`.`key` = ?",
        (format!("user:{}:stories_submitted", uid),),
    )
    .await?;

    c.exec_drop(
        "SELECT  `keystores`.* \
             FROM `keystores` \
             WHERE `keystores`.`key` = ?",
        (format!("user:{}:comments_posted", uid),),
    )
    .await?;

    c.exec_drop(
        "SELECT  1 AS one FROM `hats` \
             WHERE `hats`.`user_id` = ? LIMIT 1",
        (uid,),
    )
    .await?;

    Ok((c, true))
}

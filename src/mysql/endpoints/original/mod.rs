pub(crate) mod comment;
pub(crate) mod comment_vote;
pub(crate) mod comments;
pub(crate) mod frontpage;
pub(crate) mod recent;
pub(crate) mod story;
pub(crate) mod story_vote;
pub(crate) mod submit;
pub(crate) mod user;

use mysql_async::prelude::*;
use mysql_async::{Conn, Error};

pub(crate) async fn notifications(mut c: Conn, uid: u32) -> Result<Conn, Error> {
    c.exec_drop(
        "SELECT COUNT(*) \
                     FROM `replying_comments_for_count`
                     WHERE `replying_comments_for_count`.`user_id` = ? \
                     GROUP BY `replying_comments_for_count`.`user_id` \
                     ",
        (uid,),
    )
    .await?;

    c.exec_drop(
        "SELECT `keystores`.* \
             FROM `keystores` \
             WHERE `keystores`.`key` = ?",
        (format!("user:{}:unread_messages", uid),),
    )
    .await?;

    Ok(c)
}

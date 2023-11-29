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

pub(crate) async fn notifications(
    mut c: my::Conn,
    uid: u32,
) -> Result<my::Conn, mysql_async::Error> {
    c.exec_drop(
        "SELECT BOUNDARY_notifications.notifications
      FROM BOUNDARY_notifications
      WHERE BOUNDARY_notifications.user_id = ?",
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

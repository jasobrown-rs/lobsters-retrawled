use deadpool_postgres::{Client, Pool};
use std::collections::HashSet;
use std::future::Future;
use trawler::{StoryId, UserId};

use crate::error::{Error, Result};

pub(crate) async fn handle(
    pool: Pool,
    acting_as: Option<UserId>,
    id: StoryId,
) -> Result<(Client, bool), Error> {
    // XXX: at the end there are also a bunch of repeated, seemingly superfluous queries
    let c = pool.get().await?;
    let stmt = c
        .prep(
            "SELECT `stories`.* \
             FROM `stories` \
             WHERE `stories`.`short_id` = ?",
        )
        .await?;
    let mut story = c
        .exec_iter(stmt, (::std::str::from_utf8(&id[..]).unwrap(),))
        .await?
        .collect_and_drop::<my::Row>()
        .await?;
    let story = story.swap_remove(0);
    let author = story.get::<u32, _>("user_id").unwrap();
    let story = story.get::<u32, _>("id").unwrap();
    c.exec_drop(
        "SELECT `users`.* FROM `users` WHERE `users`.`id` = ?",
        (author,),
    )
    .await?;

    // NOTE: technically this happens before the select from user...
    if let Some(uid) = acting_as {
        // keep track of when the user last saw this story
        // NOTE: *technically* the update only happens at the end...
        let rr = c
            .query_one(
                "SELECT  `read_ribbons`.* \
                     FROM `read_ribbons` \
                     WHERE `read_ribbons`.`user_id` = ? \
                     AND `read_ribbons`.`story_id` = ?",
                &[&uid, &story],
            )
            .await?;
        let now = chrono::Local::now().naive_local();
        match rr {
            None => {
                c.execute(
                    "INSERT INTO `read_ribbons` \
                         (`created_at`, `updated_at`, `user_id`, `story_id`) \
                         VALUES (?, ?, ?, ?)",
                    &[&now, &now, &uid, &story],
                )
                .await?
            }
            Some(rr) => {
                c.execute(
                    "UPDATE `read_ribbons` \
                         SET `read_ribbons`.`updated_at` = ? \
                         WHERE `read_ribbons`.`id` = ?",
                    &[&now, &rr.get::<&str, u32>("id")],
                )
                .await?
            }
        };
    }

    // XXX: probably not drop here, but we know we have no merged stories
    c.execute(
        "SELECT `stories`.`id` \
             FROM `stories` \
             WHERE `stories`.`merged_story_id` = ?",
        &[&story],
    )
    .await?;

    let comments = c
        .prep(
            "SELECT `comments`.* \
             FROM `comments` \
             WHERE `comments`.`story_id` = ? \
             ORDER BY \
             (CAST(upvotes AS signed) - CAST(downvotes AS signed)) < 0 ASC, \
             confidence DESC",
        )
        .await?;

    let (users, comments) = c
        .exec_iter(comments, (story,))
        .await?
        .reduce_and_drop(
            (HashSet::new(), HashSet::new()),
            |(mut users, mut comments), comment: Row| {
                users.insert(comment.get::<u32, _>("user_id").unwrap());
                comments.insert(comment.get::<u32, _>("id").unwrap());
                (users, comments)
            },
        )
        .await?;

    // get user info for all commenters
    let users = users
        .into_iter()
        .map(|id| format!("{}", id))
        .collect::<Vec<_>>()
        .join(", ");
    c.simple_query(&format!(
        "SELECT `users`.* FROM `users` WHERE `users`.`id` IN ({})",
        users
    ))
    .await?;

    // get comment votes
    // XXX: why?!
    let comments = comments
        .into_iter()
        .map(|id| format!("{}", id))
        .collect::<Vec<_>>()
        .join(", ");
    c.simple_query(&format!(
        "SELECT `votes`.* FROM `votes` WHERE `votes`.`comment_id` IN ({})",
        comments
    ))
    .await?;

    // NOTE: lobste.rs here fetches the user list again. unclear why?
    if let Some(uid) = acting_as {
        c.execute(
            "SELECT `votes`.* \
                 FROM `votes` \
                 WHERE `votes`.`user_id` = ? \
                 AND `votes`.`story_id` = ? \
                 AND `votes`.`comment_id` IS NULL",
            &[&uid, &story],
        )
        .await?;
        c.execute(
            "SELECT `hidden_stories`.* \
                 FROM `hidden_stories` \
                 WHERE `hidden_stories`.`user_id` = ? \
                 AND `hidden_stories`.`story_id` = ?",
            &[&uid, &story],
        )
        .await?;
        c.simple_query(
            "SELECT `saved_stories`.* \
                 FROM `saved_stories` \
                 WHERE `saved_stories`.`user_id` = ? \
                 AND `saved_stories`.`story_id` = ?",
            &[&uid, &story],
        )
        .await?;
    }

    let stmt = c
        .prep(
            "SELECT `taggings`.* \
             FROM `taggings` \
             WHERE `taggings`.`story_id` = ?",
        )
        .await?;

    let tags = c
        .exec_iter(stmt, (story,))
        .await?
        .reduce_and_drop(HashSet::new(), |mut tags, tagging: Row| {
            tags.insert(tagging.get::<u32, _>("tag_id").unwrap());
            tags
        })
        .await?;

    let tags = tags
        .into_iter()
        .map(|id| format!("{}", id))
        .collect::<Vec<_>>()
        .join(", ");
    c.simple_query(&format!(
        "SELECT `tags`.* FROM `tags` WHERE `tags`.`id` IN ({})",
        tags
    ))
    .await?;

    Ok((c, true))
}

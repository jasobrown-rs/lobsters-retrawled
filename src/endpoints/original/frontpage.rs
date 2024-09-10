use mysql_async::prelude::*;
use mysql_async::{Conn, Error, Row};
use std::collections::HashSet;
use std::future::Future;
use std::iter;
use trawler::UserId;

pub(crate) async fn handle<F>(c: F, acting_as: Option<UserId>) -> Result<(Conn, bool), Error>
where
    F: 'static + Future<Output = Result<Conn, Error>> + Send,
{
    let mut c = c.await?;
    let stories = c
        .query_iter(
            "SELECT  `stories`.* FROM `stories` \
             WHERE `stories`.`merged_story_id` IS NULL \
             AND `stories`.`is_expired` = 0 \
             AND ((CAST(upvotes AS signed) - CAST(downvotes AS signed)) >= 0) \
             ORDER BY hotness LIMIT 51 OFFSET 0",
        )
        .await?;
    let (users, stories) = stories
        .reduce_and_drop(
            (HashSet::new(), HashSet::new()),
            |(mut users, mut stories), story: Row| {
                users.insert(story.get::<u32, _>("user_id").unwrap());
                stories.insert(story.get::<u32, _>("id").unwrap());
                (users, stories)
            },
        )
        .await?;

    assert!(!stories.is_empty(), "got no stories from /frontpage");

    let stories_in = stories
        .iter()
        .map(|id| format!("{}", id))
        .collect::<Vec<_>>()
        .join(",");

    if let Some(uid) = acting_as {
        c.exec_drop(
            "SELECT `hidden_stories`.`story_id` \
                 FROM `hidden_stories` \
                 WHERE `hidden_stories`.`user_id` = ?",
            (uid,),
        )
        .await?;

        let tags = c
            .exec_iter(
                "SELECT `tag_filters`.* FROM `tag_filters` \
                 WHERE `tag_filters`.`user_id` = ?",
                (uid,),
            )
            .await?
            .reduce_and_drop(Vec::new(), |mut tags, tag: Row| {
                tags.push(tag.get::<u32, _>("tag_id").unwrap());
                tags
            })
            .await?;

        if !tags.is_empty() {
            let tags = tags
                .into_iter()
                .map(|id| format!("{}", id))
                .collect::<Vec<_>>()
                .join(",");

            c.query_drop(format!(
                "SELECT `taggings`.`story_id` \
                     FROM `taggings` \
                     WHERE `taggings`.`story_id` IN ({}) \
                     AND `taggings`.`tag_id` IN ({})",
                stories_in, tags
            ))
            .await?;
        }
    }

    let users = users
        .into_iter()
        .map(|id| format!("{}", id))
        .collect::<Vec<_>>()
        .join(",");
    c.query_drop(format!(
        "SELECT `users`.* FROM `users` WHERE `users`.`id` IN ({})",
        users,
    ))
    .await?;

    c.query_drop(format!(
        "SELECT `suggested_titles`.* \
             FROM `suggested_titles` \
             WHERE `suggested_titles`.`story_id` IN ({})",
        stories_in
    ))
    .await?;

    c.query_drop(format!(
        "SELECT `suggested_taggings`.* \
             FROM `suggested_taggings` \
             WHERE `suggested_taggings`.`story_id` IN ({})",
        stories_in
    ))
    .await?;

    let tags = c
        .query_iter(format!(
            "SELECT `taggings`.* FROM `taggings` \
             WHERE `taggings`.`story_id` IN ({})",
            stories_in
        ))
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
        .join(",");
    c.query_drop(format!(
        "SELECT `tags`.* FROM `tags` WHERE `tags`.`id` IN ({})",
        tags
    ))
    .await?;

    // also load things that we need to highlight
    if let Some(uid) = acting_as {
        let story_params = stories.iter().map(|_| "?").collect::<Vec<_>>().join(",");
        let values: Vec<_> = iter::once(&uid as &_)
            .chain(stories.iter().map(|s| s as &_))
            .collect();
        c.exec_drop(
            format!(
                "SELECT `votes`.* FROM `votes` \
                     WHERE `votes`.`user_id` = ? \
                     AND `votes`.`story_id` IN ({}) \
                     AND `votes`.`comment_id` IS NULL",
                story_params
            ),
            values,
        )
        .await?;

        let values: Vec<_> = iter::once(&uid as &_)
            .chain(stories.iter().map(|s| s as &_))
            .collect();
        c.exec_drop(
            format!(
                "SELECT `hidden_stories`.* \
                     FROM `hidden_stories` \
                     WHERE `hidden_stories`.`user_id` = ? \
                     AND `hidden_stories`.`story_id` IN ({})",
                story_params
            ),
            values,
        )
        .await?;

        let values: Vec<_> = iter::once(&uid as &_)
            .chain(stories.iter().map(|s| s as &_))
            .collect();
        c.exec_drop(
            format!(
                "SELECT `saved_stories`.* \
                     FROM `saved_stories` \
                     WHERE `saved_stories`.`user_id` = ? \
                     AND `saved_stories`.`story_id` IN ({})",
                story_params
            ),
            values,
        )
        .await?;
    }

    Ok((c, true))
}

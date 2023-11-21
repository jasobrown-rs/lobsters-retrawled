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
    let stmt = c
        .query(
            "SELECT  `comments`.* \
             FROM `comments` \
             WHERE `comments`.`is_deleted` = 0 \
             AND `comments`.`is_moderated` = 0 \
             ORDER BY id DESC \
             LIMIT 40 OFFSET 0",
        )
        .await?;

    let (comments, users, stories) = c
        .query_iter(stmt)
        .await?
        .reduce_and_drop(
            (Vec::new(), HashSet::new(), HashSet::new()),
            |(mut comments, mut users, mut stories), comment: Row| {
                comments.push(comment.get::<u32, _>("id").unwrap());
                users.insert(comment.get::<u32, _>("user_id").unwrap());
                stories.insert(comment.get::<u32, _>("story_id").unwrap());
                (comments, users, stories)
            },
        )
        .await?;

    if let Some(uid) = acting_as {
        let params = stories.iter().map(|_| "?").collect::<Vec<_>>().join(",");
        let args: Vec<_> = iter::once(&uid as &_)
            .chain(stories.iter().map(|c| c as &_))
            .collect();
        c.exec_drop(
            &format!(
                "SELECT 1 FROM hidden_stories \
                     WHERE user_id = ? \
                     AND hidden_stories.story_id IN ({})",
                params
            ),
            args,
        )
        .await?;
    }

    let users = users
        .into_iter()
        .map(|id| format!("{}", id))
        .collect::<Vec<_>>()
        .join(",");
    c.query_drop(&format!(
        "SELECT `users`.* FROM `users` \
             WHERE `users`.`id` IN ({})",
        users
    ))
    .await?;

    let stories = stories
        .into_iter()
        .map(|id| format!("{}", id))
        .collect::<Vec<_>>()
        .join(",");

    let stmt = c
        .query(&format!(
            "SELECT  `stories`.* FROM `stories` \
             WHERE `stories`.`id` IN ({})",
            stories
        ))
        .await?;

    let authors = c
        .query_iter(stmt)
        .await?
        .reduce_and_drop(HashSet::new(), |mut authors, story: Row| {
            authors.insert(story.get::<u32, _>("user_id").unwrap());
            authors
        })
        .await?;

    if let Some(uid) = acting_as {
        let params = comments.iter().map(|_| "?").collect::<Vec<_>>().join(",");
        let comments: Vec<_> = iter::once(&uid as &_)
            .chain(comments.iter().map(|c| c as &_))
            .collect();

        c.exec_drop(
            &format!(
                "SELECT `votes`.* FROM `votes` \
                     WHERE `votes`.`user_id` = ? \
                     AND `votes`.`comment_id` IN ({})",
                params
            ),
            comments,
        )
        .await?;
    }

    // NOTE: the real website issues all of these one by one...
    let authors = authors
        .into_iter()
        .map(|id| format!("{}", id))
        .collect::<Vec<_>>()
        .join(",");

    c.query_drop(&format!(
        "SELECT  `users`.* FROM `users` \
             WHERE `users`.`id` IN ({})",
        authors
    ))
    .await?;

    Ok((c, true))
}

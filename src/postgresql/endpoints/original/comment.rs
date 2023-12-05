use deadpool_postgres::{Client, Pool};
use std::future::Future;
use trawler::{CommentId, StoryId, UserId};

use crate::error::{Error, Result};

pub(crate) async fn handle(
    pool: Pool,
    acting_as: Option<UserId>,
    id: CommentId,
    story: StoryId,
    parent: Option<CommentId>,
    priming: bool,
) -> Result<(Client, bool), Error> {
    let c = pool.get().await?;
    let user = acting_as.unwrap();
    let story = c
        .query_opt(
            "SELECT `stories`.* \
             FROM `stories` \
             WHERE `stories`.`short_id` = ?",
            &[&::std::str::from_utf8(&story[..]).unwrap()],
        )
        .await?;
    let story = story.unwrap();
    let author = story.get::<&str, u32>("user_id");
    let hotness = story.get::<&str, f64>("hotness");
    let story = story.get::<&str, u32>("id");

    if !priming {
        c.execute(
            "SELECT `users`.* FROM `users` WHERE `users`.`id` = ?",
            &[&author],
        )
        .await?;
    }

    let parent = if let Some(parent) = parent {
        // check that parent exists
        let p = c
            .query_opt(
                "SELECT  `comments`.* FROM `comments` \
                 WHERE `comments`.`story_id` = ? \
                 AND `comments`.`short_id` = ?",
                &[&story, &::std::str::from_utf8(&parent[..]).unwrap()],
            )
            .await?;

        if let Some(p) = p {
            Some((
                p.get::<&str, u32>("id"),
                p.try_get::<&str, Option<u32>>("thread_id"),
            ))
        } else {
            eprintln!(
                "failed to find parent comment {} in story {}",
                ::std::str::from_utf8(&parent[..]).unwrap(),
                story
            );
            None
        }
    } else {
        None
    };

    // TODO: real site checks for recent comments by same author with same
    // parent to ensure we don't double-post accidentally

    if !priming {
        // check that short id is available
        c.execute(
            "SELECT  1 AS one FROM `comments` \
                 WHERE `comments`.`short_id` = ?",
            &[&::std::str::from_utf8(&id[..]).unwrap()],
        )
        .await?;
    }

    // TODO: real impl checks *new* short_id *again*

    // NOTE: MySQL technically does everything inside this and_then in a transaction,
    // but let's be nice to it
    let now = chrono::Local::now().naive_local();
    let q = if let Some((parent, thread)) = parent {
        let stmt = c
            .prepare(
                "INSERT INTO `comments` \
             (`created_at`, `updated_at`, `short_id`, `story_id`, \
             `user_id`, `parent_comment_id`, `thread_id`, \
             `comment`, `upvotes`, `confidence`, \
             `markeddown_comment`) \
             VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            )
            .await?;
        c.query(
            &stmt,
            &[
                &now,
                &now,
                &::std::str::from_utf8(&id[..]).unwrap(),
                &story,
                &user,
                &parent,
                &thread,
                &"moar benchmarking", // lorem ipsum?
                &1,
                &0.1828847834138887,
                &"<p>moar benchmarking</p>\n",
            ],
        )
        .await?
    } else {
        let stmt = c
            .prepare(
                "INSERT INTO `comments` \
             (`created_at`, `updated_at`, `short_id`, `story_id`, \
             `user_id`, `comment`, `upvotes`, `confidence`, \
             `markeddown_comment`) \
             VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
            )
            .await?;
        c.query(
            &stmt,
            &[
                &now,
                &now,
                &::std::str::from_utf8(&id[..]).unwrap(),
                &story,
                &user,
                &"moar benchmarking", // lorem ipsum?
                &1,
                &0.1828847834138887,
                &"<p>moar benchmarking</p>\n",
            ],
        )
        .await?
    };
    let comment = q.last_insert_id().unwrap();

    if !priming {
        // but why?!
        c.execute(
            "SELECT  `votes`.* FROM `votes` \
                 WHERE `votes`.`user_id` = ? \
                 AND `votes`.`story_id` = ? \
                 AND `votes`.`comment_id` = ?",
            &[&user, &story, &comment],
        )
        .await?;
    }

    c.execute(
        "INSERT INTO `votes` \
             (`user_id`, `story_id`, `comment_id`, `vote`) \
             VALUES (?, ?, ?, ?)",
        &[&user, &story, &comment, &1],
    )
    .await?;

    c.execute(
        "SELECT `stories`.`id` \
             FROM `stories` \
             WHERE `stories`.`merged_story_id` = ?",
        &[&story],
    )
    .await?;

    // why are these ordered?
    let stmt = c
        .prepare(
            "SELECT `comments`.*, \
             `comments`.`upvotes` - `comments`.`downvotes` AS saldo \
             FROM `comments` \
             WHERE `comments`.`story_id` = ? \
             ORDER BY \
             saldo ASC, \
             confidence DESC",
        )
        .await?;
    let count = c
        .query(&stmt, &[&story])
        .await?
        .into_iter()
        .fold(0, |rows, _| rows + 1);

    c.execute(
        "UPDATE `stories` \
         SET `comments_count` = ?
         WHERE `stories`.`id` = ?",
        &[&count, &story],
    )
    .await?;

    if !priming {
        // get all the stuff needed to compute updated hotness
        c.execute(
            "SELECT `tags`.* \
                 FROM `tags` \
                 INNER JOIN `taggings` \
                 ON `tags`.`id` = `taggings`.`tag_id` \
                 WHERE `taggings`.`story_id` = ?",
            &[&story],
        )
        .await?;

        c.execute(
            "SELECT \
                 `comments`.`upvotes`, \
                 `comments`.`downvotes` \
                 FROM `comments` \
                 JOIN `stories` ON (`stories`.`id` = `comments`.`story_id`) \
                 WHERE `comments`.`story_id` = ? \
                 AND `comments`.`user_id` <> `stories`.`user_id`",
            &[&story],
        )
        .await?;

        c.execute(
            "SELECT `stories`.`id` \
                 FROM `stories` \
                 WHERE `stories`.`merged_story_id` = ?",
            &[&story],
        )
        .await?;
    }

    // why oh why is story hotness *updated* here?!
    c.execute(
        "UPDATE `stories` \
             SET `hotness` = ? \
             WHERE `stories`.`id` = ?",
        &[&(hotness - 1.0), &story],
    )
    .await?;

    let key = format!("user:{}:comments_posted", user);
    c.execute(
        "INSERT INTO keystores (`key`, `value`) \
             VALUES (?, ?) \
             ON DUPLICATE KEY UPDATE `keystores`.`value` = `keystores`.`value` + 1",
        &[&key, &1],
    )
    .await?;

    Ok((c, false))
}

use mysql_async::prelude::*;
use mysql_async::{Conn, Error, Row};
use std::future::Future;
use trawler::{CommentId, StoryId, UserId};

pub(crate) async fn handle<F>(
    c: F,
    acting_as: Option<UserId>,
    id: CommentId,
    story: StoryId,
    parent: Option<CommentId>,
    priming: bool,
) -> Result<(Conn, bool), Error>
where
    F: 'static + Future<Output = Result<Conn, Error>> + Send,
{
    let mut c = c.await?;
    let user = acting_as.unwrap();
    let story = c
        .exec_first::<Row, _, _>(
            "SELECT `stories`.* \
             FROM `stories` \
             WHERE `stories`.`short_id` = ?",
            (::std::str::from_utf8(&story[..]).unwrap(),),
        )
        .await?;
    let story = story.unwrap();
    let author = story.get::<u32, _>("user_id").unwrap();
    let hotness = story.get::<f64, _>("hotness").unwrap();
    let story = story.get::<u32, _>("id").unwrap();

    if !priming {
        c.exec_drop(
            "SELECT `users`.* FROM `users` WHERE `users`.`id` = ?",
            (author,),
        )
        .await?;
    }

    let parent = if let Some(parent) = parent {
        // check that parent exists
        let p = c
            .exec_first::<Row, _, _>(
                "SELECT  `comments`.* FROM `comments` \
                 WHERE `comments`.`story_id` = ? \
                 AND `comments`.`short_id` = ?",
                (story, ::std::str::from_utf8(&parent[..]).unwrap()),
            )
            .await?;

        if let Some(p) = p {
            Some((
                p.get::<u32, _>("id").unwrap(),
                p.get::<Option<u32>, _>("thread_id").unwrap(),
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
        c.exec_drop(
            "SELECT  1 AS one FROM `comments` \
                 WHERE `comments`.`short_id` = ?",
            (::std::str::from_utf8(&id[..]).unwrap(),),
        )
        .await?;
    }

    // TODO: real impl checks *new* short_id *again*

    // NOTE: MySQL technically does everything inside this and_then in a transaction,
    // but let's be nice to it
    let now = chrono::Local::now().naive_local();
    let q = if let Some((parent, thread)) = parent {
        c.exec_iter(
            "INSERT INTO `comments` \
             (`created_at`, `updated_at`, `short_id`, `story_id`, \
             `user_id`, `parent_comment_id`, `thread_id`, \
             `comment`, `upvotes`, `confidence`, \
             `markeddown_comment`) \
             VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (
                now,
                now,
                ::std::str::from_utf8(&id[..]).unwrap(),
                story,
                user,
                parent,
                thread,
                "moar benchmarking", // lorem ipsum?
                1,
                0.1828847834138887,
                "<p>moar benchmarking</p>\n",
            ),
        )
        .await?
    } else {
        c.exec_iter(
            "INSERT INTO `comments` \
             (`created_at`, `updated_at`, `short_id`, `story_id`, \
             `user_id`, `comment`, `upvotes`, `confidence`, \
             `markeddown_comment`) \
             VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (
                now,
                now,
                ::std::str::from_utf8(&id[..]).unwrap(),
                story,
                user,
                "moar benchmarking", // lorem ipsum?
                1,
                0.1828847834138887,
                "<p>moar benchmarking</p>\n",
            ),
        )
        .await?
    };
    let comment = q.last_insert_id().unwrap();
    q.drop_result().await?;

    if !priming {
        // but why?!
        c.exec_drop(
            "SELECT  `votes`.* FROM `votes` \
                 WHERE `votes`.`user_id` = ? \
                 AND `votes`.`story_id` = ? \
                 AND `votes`.`comment_id` = ?",
            (user, story, comment),
        )
        .await?;
    }

    c.exec_drop(
        "INSERT INTO `votes` \
             (`user_id`, `story_id`, `comment_id`, `vote`) \
             VALUES (?, ?, ?, ?)",
        (user, story, comment, 1),
    )
    .await?;

    c.exec_drop(
        "SELECT `stories`.`id` \
             FROM `stories` \
             WHERE `stories`.`merged_story_id` = ?",
        (story,),
    )
    .await?;

    // why are these ordered?
    let count = c
        .exec_iter(
            "SELECT `comments`.*, \
             `comments`.`upvotes` - `comments`.`downvotes` AS saldo \
             FROM `comments` \
             WHERE `comments`.`story_id` = ? \
             ORDER BY \
             saldo ASC, \
             confidence DESC",
            (story,),
        )
        .await?
        .reduce_and_drop(0, |rows, _: Row| rows + 1)
        .await?;

    c.exec_drop(
        "UPDATE `stories` \
         SET `comments_count` = ?
         WHERE `stories`.`id` = ?",
        (count, story),
    )
    .await?;

    if !priming {
        // get all the stuff needed to compute updated hotness
        c.exec_drop(
            "SELECT `tags`.* \
                 FROM `tags` \
                 INNER JOIN `taggings` \
                 ON `tags`.`id` = `taggings`.`tag_id` \
                 WHERE `taggings`.`story_id` = ?",
            (story,),
        )
        .await?;

        c.exec_drop(
            "SELECT \
                 `comments`.`upvotes`, \
                 `comments`.`downvotes` \
                 FROM `comments` \
                 JOIN `stories` ON (`stories`.`id` = `comments`.`story_id`) \
                 WHERE `comments`.`story_id` = ? \
                 AND `comments`.`user_id` <> `stories`.`user_id`",
            (story,),
        )
        .await?;

        c.exec_drop(
            "SELECT `stories`.`id` \
                 FROM `stories` \
                 WHERE `stories`.`merged_story_id` = ?",
            (story,),
        )
        .await?;
    }

    // why oh why is story hotness *updated* here?!
    c.exec_drop(
        "UPDATE `stories` \
             SET `hotness` = ? \
             WHERE `stories`.`id` = ?",
        (hotness - 1.0, story),
    )
    .await?;

    let key = format!("user:{}:comments_posted", user);
    c.exec_drop(
        "INSERT INTO keystores (`key`, `value`) \
             VALUES (?, ?) \
             ON DUPLICATE KEY UPDATE `keystores`.`value` = `keystores`.`value` + 1",
        (key, 1),
    )
    .await?;

    Ok((c, false))
}
use deadpool_postgres::{Client, Pool};
use std::future::Future;
use trawler::{StoryId, UserId, Vote};

use crate::error::{Error, Result};

pub(crate) async fn handle(
    pool: Pool,
    acting_as: Option<UserId>,
    comment: StoryId,
    v: Vote,
) -> Result<(Client, bool), Error> {
    let c = pool.get().await?;
    let user = acting_as.unwrap();

    let comment = c
        .query_one(
            "SELECT `comments`.* \
             FROM `comments` \
             WHERE `comments`.`short_id` = $1",
            &[&::std::str::from_utf8(&comment[..]).unwrap()],
        )
        .await?;

    let author = comment.get::<&str, u32>("user_id");
    let sid = comment.get::<&str, u32>("story_id");
    let upvotes = comment.get::<&str, u32>("upvotes");
    let downvotes = comment.get::<&str, u32>("downvotes");
    let comment = comment.get::<&str, u32>("id");
    c.execute(
        "SELECT  `votes`.* \
             FROM `votes` \
             WHERE `votes`.`user_id` = ? \
             AND `votes`.`story_id` = ? \
             AND `votes`.`comment_id` = ?",
        &[&user, &sid, &comment],
    )
    .await?;

    // TODO: do something else if user has already voted
    // TODO: technically need to re-load comment under transaction

    // NOTE: MySQL technically does everything inside this and_then in a transaction,
    // but let's be nice to it
    c.execute(
        "INSERT INTO `votes` \
             (`user_id`, `story_id`, `comment_id`, `vote`) \
             VALUES \
             (?, ?, ?, ?)",
        &[
            &user,
            &sid,
            &comment,
            match v {
                Vote::Up => 1,
                Vote::Down => 0,
            },
        ],
    )
    .await?;

    c.execute(
        &format!(
            "UPDATE `users` \
                 SET `users`.`karma` = `users`.`karma` {} \
                 WHERE `users`.`id` = ?",
            match v {
                Vote::Up => "+ 1",
                Vote::Down => "- 1",
            }
        ),
        &[&author],
    )
    .await?;

    // approximate Comment::calculate_hotness
    let confidence = upvotes as f64 / (upvotes as f64 + downvotes as f64);
    c.execute(
        &format!(
            "UPDATE `comments` \
                 SET \
                 `comments`.`upvotes` = `comments`.`upvotes` {}, \
                 `comments`.`downvotes` = `comments`.`downvotes` {}, \
                 `comments`.`confidence` = ? \
                 WHERE `id` = ?",
            match v {
                Vote::Up => "+ 1",
                Vote::Down => "+ 0",
            },
            match v {
                Vote::Up => "+ 0",
                Vote::Down => "+ 1",
            },
        ),
        &[&confidence, &comment],
    )
    .await?;

    // get all the stuff needed to compute updated hotness
    let story = c
        .query_one(
            "SELECT `stories`.* \
             FROM `stories` \
             WHERE `stories`.`id` = ?",
            &[&sid],
        )
        .await?;
    let score = story.get::<&str, f64>("hotness");

    c.execute(
        "SELECT `tags`.* \
             FROM `tags` \
             INNER JOIN `taggings` ON `tags`.`id` = `taggings`.`tag_id` \
             WHERE `taggings`.`story_id` = ?",
        &[&sid],
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
        &[&sid],
    )
    .await?;

    c.execute(
        "SELECT `stories`.`id` \
             FROM `stories` \
             WHERE `stories`.`merged_story_id` = ?",
        &[&sid],
    )
    .await?;

    // the *actual* algorithm for computing hotness isn't all
    // that interesting to us. it does affect what's on the
    // frontpage, but we're okay with using a more basic
    // upvote/downvote ratio thingy. See Story::calculated_hotness
    // in the lobsters source for details.
    c.execute(
        &format!(
            "UPDATE stories SET \
                 stories.upvotes = stories.upvotes {}, \
                 stories.downvotes = stories.downvotes {}, \
                 stories.hotness = ? \
                 WHERE id = ?",
            match v {
                Vote::Up => "+ 1",
                Vote::Down => "+ 0",
            },
            match v {
                Vote::Up => "+ 0",
                Vote::Down => "+ 1",
            },
        ),
        &[
            &(score
                - match v {
                    Vote::Up => 1.0,
                    Vote::Down => -1.0,
                }),
            &sid,
        ],
    )
    .await?;

    Ok((c, false))
}

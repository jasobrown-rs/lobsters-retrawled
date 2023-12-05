use deadpool_postgres::{Client, Pool};
use std::future::Future;
use trawler::{StoryId, UserId, Vote};

use crate::error::{Error, Result};

pub(crate) async fn handle(
    pool: Pool,
    acting_as: Option<UserId>,
    story: StoryId,
    v: Vote,
) -> Result<(Client, bool), Error> {
    let c = pool.get().await?;
    let user = acting_as.unwrap();
    let stmt = c
        .prep(
            "SELECT `stories`.* \
             FROM `stories` \
             WHERE `stories`.`short_id` = ?",
        )
        .await?;
    let mut story = c
        .exec_iter(stmt, (::std::str::from_utf8(&story[..]).unwrap(),))
        .await?
        .collect_and_drop::<Row>()
        .await?;
    let story = story.swap_remove(0);

    let author = story.get::<u32, _>("user_id").unwrap();
    let score = story.get::<f64, _>("hotness").unwrap();
    let story = story.get::<u32, _>("id").unwrap();
    c.execute(
        "SELECT  `votes`.* \
             FROM `votes` \
             WHERE `votes`.`user_id` = ? \
             AND `votes`.`story_id` = ? \
             AND `votes`.`comment_id` IS NULL",
        &[&user, &story],
    )
    .await?;

    // TODO: do something else if user has already voted
    // TODO: technically need to re-load story under transaction

    // NOTE: MySQL technically does everything inside this and_then in a transaction,
    // but let's be nice to it
    c.execute(
        "INSERT INTO `votes` \
             (`user_id`, `story_id`, `vote`) \
             VALUES \
             (?, ?, ?)",
        &[
            &user,
            &story,
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

    // get all the stuff needed to compute updated hotness
    c.execute(
        "SELECT `tags`.* \
             FROM `tags` \
             INNER JOIN `taggings` ON `tags`.`id` = `taggings`.`tag_id` \
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
                 WHERE stories.id = ?",
            match v {
                Vote::Up => "+ 1",
                Vote::Down => "+ 0",
            },
            match v {
                Vote::Up => "+ 0",
                Vote::Down => "+ 1",
            },
        ),
        [
            &(score
                - match v {
                    Vote::Up => 1.0,
                    Vote::Down => -1.0,
                }),
            &story,
        ],
    )
    .await?;

    Ok((c, false))
}

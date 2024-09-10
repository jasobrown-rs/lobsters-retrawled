use mysql_async::prelude::*;
use mysql_async::{Conn, Error, Row};
use std::future::Future;
use trawler::{StoryId, UserId, Vote};

pub(crate) async fn handle<F>(
    c: F,
    acting_as: Option<UserId>,
    comment: StoryId,
    v: Vote,
) -> Result<(Conn, bool), Error>
where
    F: 'static + Future<Output = Result<Conn, Error>> + Send,
{
    let mut c = c.await?;
    let user = acting_as.unwrap();

    let comment = c
        .exec_first::<Row, _, _>(
            "SELECT `comments`.* \
             FROM `comments` \
             WHERE `comments`.`short_id` = ?",
            (::std::str::from_utf8(&comment[..]).unwrap(),),
        )
        .await?;

    let comment = comment.unwrap();
    let author = comment.get::<u32, _>("user_id").unwrap();
    let sid = comment.get::<u32, _>("story_id").unwrap();
    let upvotes = comment.get::<u32, _>("upvotes").unwrap();
    let downvotes = comment.get::<u32, _>("downvotes").unwrap();
    let comment = comment.get::<u32, _>("id").unwrap();
    c.exec_drop(
        "SELECT  `votes`.* \
             FROM `votes` \
             WHERE `votes`.`user_id` = ? \
             AND `votes`.`story_id` = ? \
             AND `votes`.`comment_id` = ?",
        (user, sid, comment),
    )
    .await?;

    // TODO: do something else if user has already voted
    // TODO: technically need to re-load comment under transaction

    // NOTE: MySQL technically does everything inside this and_then in a transaction,
    // but let's be nice to it
    c.exec_drop(
        "INSERT INTO `votes` \
             (`user_id`, `story_id`, `comment_id`, `vote`) \
             VALUES \
             (?, ?, ?, ?)",
        (
            user,
            sid,
            comment,
            match v {
                Vote::Up => 1,
                Vote::Down => 0,
            },
        ),
    )
    .await?;

    c.exec_drop(
        format!(
            "UPDATE `users` \
                 SET `users`.`karma` = `users`.`karma` {} \
                 WHERE `users`.`id` = ?",
            match v {
                Vote::Up => "+ 1",
                Vote::Down => "- 1",
            }
        ),
        (author,),
    )
    .await?;

    // approximate Comment::calculate_hotness
    let confidence = upvotes as f64 / (upvotes as f64 + downvotes as f64);
    c.exec_drop(
        format!(
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
        (confidence, comment),
    )
    .await?;

    // get all the stuff needed to compute updated hotness
    let story = c
        .exec_first::<Row, _, _>(
            "SELECT `stories`.* \
             FROM `stories` \
             WHERE `stories`.`id` = ?",
            (sid,),
        )
        .await?;
    let story = story.unwrap();
    let score = story.get::<f64, _>("hotness").unwrap();

    c.exec_drop(
        "SELECT `tags`.* \
             FROM `tags` \
             INNER JOIN `taggings` ON `tags`.`id` = `taggings`.`tag_id` \
             WHERE `taggings`.`story_id` = ?",
        (sid,),
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
        (sid,),
    )
    .await?;

    c.exec_drop(
        "SELECT `stories`.`id` \
             FROM `stories` \
             WHERE `stories`.`merged_story_id` = ?",
        (sid,),
    )
    .await?;

    // the *actual* algorithm for computing hotness isn't all
    // that interesting to us. it does affect what's on the
    // frontpage, but we're okay with using a more basic
    // upvote/downvote ratio thingy. See Story::calculated_hotness
    // in the lobsters source for details.
    c.exec_drop(
        format!(
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
        (
            score
                - match v {
                    Vote::Up => 1.0,
                    Vote::Down => -1.0,
                },
            sid,
        ),
    )
    .await?;

    Ok((c, false))
}

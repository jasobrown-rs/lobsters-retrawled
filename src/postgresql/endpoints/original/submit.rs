use deadpool_postgres::{Client, Pool};
use std::future::Future;
use trawler::{StoryId, UserId};

use crate::error::{Error, Result};

pub(crate) async fn handle(
    pool: Pool,
    acting_as: Option<UserId>,
    id: StoryId,
    title: String,
    priming: bool,
) -> Result<(Client, bool), Error> {
    let c = pool.get().await?;
    // let user = acting_as.unwrap();

    // // check that tags are active
    // let tag = c
    //     .query_first::<Row, _>(
    //         "SELECT  `tags`.* FROM `tags` \
    //          WHERE `tags`.`inactive` = 0 AND `tags`.`tag` IN ('test')",
    //     )
    //     .await?;
    // let tag = tag.unwrap().get::<u32, _>("id");

    // if !priming {
    //     // check that story id isn't already assigned
    //     c.exec_drop(
    //         "SELECT  1 AS one FROM `stories` \
    //              WHERE `stories`.`short_id` = ?",
    //         (::std::str::from_utf8(&id[..]).unwrap(),),
    //     )
    //     .await?;
    // }

    // // TODO: check for similar stories if there's a url
    // // SELECT  `stories`.*
    // // FROM `stories`
    // // WHERE `stories`.`url` IN (
    // //  'https://google.com/test',
    // //  'http://google.com/test',
    // //  'https://google.com/test/',
    // //  'http://google.com/test/',
    // //  ... etc
    // // )
    // // AND (is_expired = 0 OR is_moderated = 1)

    // // TODO
    // // real impl queries `tags` and `users` again here..?

    // // TODO: real impl checks *new* short_id and duplicate urls *again*
    // // TODO: sometimes submit url

    // // NOTE: MySQL technically does everything inside this and_then in a transaction,
    // // but let's be nice to it
    // let stmt = c
    //     .prep(
    //         "INSERT INTO `stories` \
    //          (`created_at`, `user_id`, `title`, \
    //          `description`, `short_id`, `upvotes`, `hotness`, \
    //          `markeddown_description`) \
    //          VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
    //     )
    //     .await?;
    // let q = c
    //     .exec_iter(
    //         stmt,
    //         (
    //             chrono::Local::now().naive_local(),
    //             user,
    //             title,
    //             "to infinity", // lorem ipsum?
    //             ::std::str::from_utf8(&id[..]).unwrap(),
    //             1,
    //             -19216.2884921,
    //             "<p>to infinity</p>\n",
    //         ),
    //     )
    //     .await?;
    // let story = q.last_insert_id().unwrap();
    // q.drop_result().await?;

    // c.exec_drop(
    //     "INSERT INTO `taggings` (`story_id`, `tag_id`) \
    //          VALUES (?, ?)",
    //     (story, tag),
    // )
    // .await?;

    // let key = format!("user:{}:stories_submitted", user);
    // c.exec_drop(
    //     "INSERT INTO keystores (`key`, `value`) \
    //          VALUES (?, ?) \
    //          ON DUPLICATE KEY UPDATE `keystores`.`value` = `keystores`.`value` + 1",
    //     (key, 1),
    // )
    // .await?;

    // if !priming {
    //     let key = format!("user:{}:stories_submitted", user);
    //     c.exec_drop(
    //         "SELECT  `keystores`.* \
    //              FROM `keystores` \
    //              WHERE `keystores`.`key` = ?",
    //         (key,),
    //     )
    //     .await?;

    //     c.exec_drop(
    //         "SELECT  `votes`.* FROM `votes` \
    //              WHERE `votes`.`user_id` = ? \
    //              AND `votes`.`story_id` = ? \
    //              AND `votes`.`comment_id` IS NULL",
    //         (user, story),
    //     )
    //     .await?;
    // }

    // c.exec_drop(
    //     "INSERT INTO `votes` (`user_id`, `story_id`, `vote`) \
    //          VALUES (?, ?, ?)",
    //     (user, story, 1),
    // )
    // .await?;

    // if !priming {
    //     c.exec_drop(
    //         "SELECT \
    //              `comments`.`upvotes`, \
    //              `comments`.`downvotes` \
    //              FROM `comments` \
    //              JOIN `stories` ON (`stories`.`id` = `comments`.`story_id`) \
    //              WHERE `comments`.`story_id` = ? \
    //              AND `comments`.`user_id` <> `stories`.`user_id`",
    //         (story,),
    //     )
    //     .await?;

    //     // why oh why is story hotness *updated* here?!
    //     c.exec_drop(
    //         "UPDATE `stories` \
    //              SET `hotness` = ? \
    //              WHERE `stories`.`id` = ?",
    //         (-19216.5479744, story),
    //     )
    //     .await?;
    // }

    Ok((c, false))
}

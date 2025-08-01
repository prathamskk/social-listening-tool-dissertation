CREATE OR REPLACE VIEW `social-listening-sense.social_listening_data.unified_social_content_items` AS

WITH
  -- CTE for Reddit Posts: Rely on t.post_id for stable_post_id, filter out rows where post_id is NULL.
  reddit_posts_base AS (
    SELECT
        t.post_id,
        t.url,
        t.user_posted,
        t.title,
        t.description,
        t.num_upvotes,
        t.date_posted,
        t.community_name,
        t.photos,
        t.videos,
        t.timestamp,
        t.comments,
        t.snapshot_id,
        t.error_code,
        t.error,
        t.warning_code,
        t.warning,
        t.post_id AS stable_post_id
    FROM
      `social-listening-sense.social_listening_data.reddit_data` AS t
    WHERE
      t.post_id IS NOT NULL
      AND (t.title IS NOT NULL OR t.description IS NOT NULL)
  ),

  -- CTE for Reddit Comments: Generate stable comment_item_id without UUID.
  reddit_comments_base AS (
    SELECT
        p.stable_post_id,
        c.url AS comment_url,
        c.user_commenting,
        c.comment,
        c.num_upvotes,
        c.date_of_comment,
        c.replies,
        -- Generate a stable ID for the comment using deterministic fields
        CONCAT('reddit_comment_gen_', FARM_FINGERPRINT(
            CONCAT(
                COALESCE(p.stable_post_id, ''),
                COALESCE(c.user_commenting, ''),
                FORMAT_TIMESTAMP('%Y%m%d%H%M%S%F', COALESCE(c.date_of_comment, CAST('1970-01-01 00:00:00 UTC' AS TIMESTAMP))),
                COALESCE(c.comment, '')
            )
        )) AS stable_comment_id
    FROM
      reddit_posts_base AS p,
      UNNEST(p.comments) AS c
    WHERE
      c.comment IS NOT NULL
  ),

  -- CTE for Reddit Replies: Generate stable reply_item_id without UUID.
  reddit_replies_base AS (
    SELECT
        c.stable_post_id,
        c.stable_comment_id,
        r.user_url,
        r.user_replying,
        r.num_upvotes,
        r.date_of_reply,
        r.reply AS reply_content,
        -- Generate a stable ID for the reply using deterministic fields
        CONCAT('reddit_reply_gen_', FARM_FINGERPRINT(
            CONCAT(
                COALESCE(c.stable_comment_id, ''),
                COALESCE(r.user_replying, ''),
                FORMAT_TIMESTAMP('%Y%m%d%H%M%S%F', COALESCE(r.date_of_reply, CAST('1970-01-01 00:00:00 UTC' AS TIMESTAMP))),
                COALESCE(r.user_replying, '') --  Used for hash
            )
        )) AS stable_reply_id
    FROM
      reddit_comments_base AS c,
      UNNEST(c.replies) AS r
    WHERE
      r.reply IS NOT NULL -- Filtering on user_replying,
  ),

  -- CTE for Quora Questions: Rely on t.post_id for stable_post_id, filter out rows where post_id is NULL.
  quora_questions_base AS (
    SELECT
        t.post_id,
        t.url,
        t.author_name,
        t.title,
        t.post_text,
        t.upvotes,
        t.post_date,
        t.pictures_urls,
        t.videos_urls,
        t.top_comments,
        t.timestamp,
        t.snapshot_id,
        t.post_id AS stable_post_id
    FROM
      `social-listening-sense.social_listening_data.quora_data` AS t
    WHERE
      t.post_id IS NOT NULL
      AND (t.title IS NOT NULL OR t.post_text IS NOT NULL)
  ),

  -- CTE for Quora Comments: Generate stable comment_item_id.
  quora_comments_base AS (
    SELECT
        q.stable_post_id,
        q.url AS question_url,
        c.commenter_name,
        c.comment,
        c.comment_date,
        c.replys,
        -- Generate a stable ID for the comment
        CONCAT('quora_comment_gen_', FARM_FINGERPRINT(
            CONCAT(
                COALESCE(q.stable_post_id, ''),
                COALESCE(c.commenter_name, ''),
                FORMAT_TIMESTAMP('%Y%m%d%H%M%S%F', COALESCE(c.comment_date, CAST('1970-01-01 00:00:00 UTC' AS TIMESTAMP))),
                COALESCE(c.comment, '')
            )
        )) AS stable_comment_id
    FROM
      quora_questions_base AS q,
      UNNEST(q.top_comments) AS c
    WHERE
      c.comment IS NOT NULL
  ),

  -- CTE for Quora Replies: Generate stable reply_item_id.
  quora_replies_base AS (
    SELECT
        c.stable_post_id,
        c.stable_comment_id,
        r.commenter_name,
        r.comment,
        r.comment_date,
        -- Generate a stable ID for the reply
        CONCAT('quora_reply_gen_', FARM_FINGERPRINT(
            CONCAT(
                COALESCE(c.stable_comment_id, ''),
                COALESCE(r.commenter_name, ''),
                FORMAT_TIMESTAMP('%Y%m%d%H%M%S%F', COALESCE(r.comment_date, CAST('1970-01-01 00:00:00 UTC' AS TIMESTAMP))),
                COALESCE(r.comment, '')
            )
        )) AS stable_reply_id
    FROM
      quora_comments_base AS c,
      UNNEST(c.replys) AS r
    WHERE
      r.comment IS NOT NULL
  )

--- Final UNION ALL for the Unified View ---
SELECT * FROM (
  SELECT
      'Reddit' AS source,
      'post' AS content_type,
      t.stable_post_id AS content_item_id,
      CAST(NULL AS STRING) AS parent_content_item_id,
      t.stable_post_id AS top_level_post_id,
      t.url AS content_item_url,
      t.user_posted AS author_username,
      t.title AS primary_text,
      t.description AS full_text_context,
      CAST(t.num_upvotes AS INT64) AS engagement_score,
      t.date_posted AS content_timestamp,
      t.community_name AS community_or_channel_name,
      ARRAY<STRING>[] AS hashtags,
      ARRAY<STRING>[] AS mentions,
      t.photos AS media_urls,
      t.videos AS video_urls,
      t.timestamp AS record_load_timestamp,
      t.snapshot_id,
      ROW_NUMBER() OVER(PARTITION BY t.stable_post_id ORDER BY t.date_posted DESC) as rn
  FROM
      reddit_posts_base AS t
)
WHERE rn = 1

UNION ALL

SELECT * FROM (
  SELECT
      'Reddit' AS source,
      'comment' AS content_type,
      c.stable_comment_id AS content_item_id,
      c.stable_post_id AS parent_content_item_id,
      c.stable_post_id AS top_level_post_id,
      c.comment_url AS content_item_url,
      c.user_commenting AS author_username,
      c.comment AS primary_text,
      CONCAT(COALESCE(rp.title, ''), ' ', COALESCE(rp.description, ''), ' ', COALESCE(c.comment, '')) AS full_text_context,
      CAST(c.num_upvotes AS INT64) AS engagement_score,
      c.date_of_comment AS content_timestamp,
      rp.community_name AS community_or_channel_name,
      ARRAY<STRING>[] AS hashtags,
      ARRAY<STRING>[] AS mentions,
      ARRAY<STRING>[] AS media_urls,
      ARRAY<STRING>[] AS video_urls,
      rp.timestamp AS record_load_timestamp,
      rp.snapshot_id,
      ROW_NUMBER() OVER(PARTITION BY c.stable_comment_id ORDER BY c.date_of_comment DESC, rp.date_posted DESC) as rn
  FROM
      reddit_comments_base AS c
  JOIN
      reddit_posts_base AS rp
  ON
      c.stable_post_id = rp.stable_post_id
)
WHERE rn = 1

UNION ALL

SELECT * FROM (
  SELECT
      'Reddit' AS source,
      'reply' AS content_type,
      r.stable_reply_id AS content_item_id,
      r.stable_comment_id AS parent_content_item_id,
      r.stable_post_id AS top_level_post_id,
      r.user_url AS content_item_url,
      r.user_replying AS author_username,
      r.reply_content AS primary_text, 
      CONCAT(COALESCE(rp.title, ''), ' ', COALESCE(rp.description, ''), ' ', COALESCE(rc.comment, ''), ' ', COALESCE(r.reply_content, '')) AS full_text_context, 
      CAST(r.num_upvotes AS INT64) AS engagement_score,
      r.date_of_reply AS content_timestamp,
      rp.community_name AS community_or_channel_name,
      ARRAY<STRING>[] AS hashtags,
      ARRAY<STRING>[] AS mentions,
      ARRAY<STRING>[] AS media_urls,
      ARRAY<STRING>[] AS video_urls,
      rp.timestamp AS record_load_timestamp,
      rp.snapshot_id,
      ROW_NUMBER() OVER(PARTITION BY r.stable_reply_id ORDER BY r.date_of_reply DESC, rc.date_of_comment DESC, rp.date_posted DESC) as rn
  FROM
      reddit_replies_base AS r
  JOIN
      reddit_comments_base AS rc
  ON
      r.stable_comment_id = rc.stable_comment_id
  JOIN
      reddit_posts_base AS rp
  ON
      r.stable_post_id = rp.stable_post_id
)
WHERE rn = 1

UNION ALL

SELECT * FROM (
  SELECT
      'Quora' AS source,
      'post' AS content_type,
      t.stable_post_id AS content_item_id,
      CAST(NULL AS STRING) AS parent_content_item_id,
      t.stable_post_id AS top_level_post_id,
      t.url AS content_item_url,
      t.author_name AS author_username,
      t.title AS primary_text,
      t.post_text AS full_text_context,
      CAST(t.upvotes AS INT64) AS engagement_score,
      t.post_date AS content_timestamp,
      CAST(NULL AS STRING) AS community_or_channel_name,
      ARRAY<STRING>[] AS hashtags,
      ARRAY<STRING>[] AS mentions,
      t.pictures_urls AS media_urls,
      CASE WHEN t.videos_urls IS NOT NULL THEN [t.videos_urls] ELSE ARRAY<STRING>[] END AS video_urls,
      t.timestamp AS record_load_timestamp,
      t.snapshot_id,
      ROW_NUMBER() OVER(PARTITION BY t.stable_post_id ORDER BY t.post_date DESC) as rn
  FROM
      quora_questions_base AS t
)
WHERE rn = 1

UNION ALL

SELECT * FROM (
  SELECT
      'Quora' AS source,
      'comment' AS content_type,
      c.stable_comment_id AS content_item_id,
      c.stable_post_id AS parent_content_item_id,
      c.stable_post_id AS top_level_post_id,
      q.url AS content_item_url,
      c.commenter_name AS author_username,
      c.comment AS primary_text,
      CONCAT(COALESCE(q.title, ''), ' ', COALESCE(q.post_text, ''), ' ', COALESCE(c.comment, '')) AS full_text_context,
      CAST(NULL AS INT64) AS engagement_score,
      c.comment_date AS content_timestamp,
      CAST(NULL AS STRING) AS community_or_channel_name,
      ARRAY<STRING>[] AS hashtags,
      ARRAY<STRING>[] AS mentions,
      ARRAY<STRING>[] AS media_urls,
      ARRAY<STRING>[] AS video_urls,
      q.timestamp AS record_load_timestamp,
      q.snapshot_id,
      ROW_NUMBER() OVER(PARTITION BY c.stable_comment_id ORDER BY c.comment_date DESC, q.post_date DESC) as rn
  FROM
      quora_comments_base AS c
  JOIN
      quora_questions_base AS q
  ON
      c.stable_post_id = q.stable_post_id
)
WHERE rn = 1

UNION ALL

SELECT * FROM (
  SELECT
      'Quora' AS source,
      'reply' AS content_type,
      r.stable_reply_id AS content_item_id,
      r.stable_comment_id AS parent_content_item_id,
      r.stable_post_id AS top_level_post_id,
      q.url AS content_item_url,
      r.commenter_name AS author_username,
      r.comment AS primary_text,
      CONCAT(COALESCE(q.title, ''), ' ', COALESCE(q.post_text, ''), ' ', COALESCE(qc.comment, ''), ' ', COALESCE(r.comment, '')) AS full_text_context,
      CAST(NULL AS INT64) AS engagement_score,
      r.comment_date AS content_timestamp,
      CAST(NULL AS STRING) AS community_or_channel_name,
      ARRAY<STRING>[] AS hashtags,
      ARRAY<STRING>[] AS mentions,
      ARRAY<STRING>[] AS media_urls,
      ARRAY<STRING>[] AS video_urls,
      q.timestamp AS record_load_timestamp,
      q.snapshot_id,
      ROW_NUMBER() OVER(PARTITION BY r.stable_reply_id ORDER BY r.comment_date DESC, qc.comment_date DESC, q.post_date DESC) as rn
  FROM
      quora_replies_base AS r
  JOIN
      quora_comments_base AS qc
  ON
      r.stable_comment_id = qc.stable_comment_id
  JOIN
      quora_questions_base AS q
  ON
      r.stable_post_id = q.stable_post_id
)
WHERE rn = 1
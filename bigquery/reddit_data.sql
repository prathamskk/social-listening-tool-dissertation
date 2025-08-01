[
  {
    "name": "error_code",
    "mode": "NULLABLE",
    "type": "STRING",
    "description": "",
    "fields": []
  },
  {
    "name": "error",
    "mode": "NULLABLE",
    "type": "STRING",
    "description": "",
    "fields": []
  },
  {
    "name": "warning_code",
    "mode": "NULLABLE",
    "type": "STRING",
    "description": "",
    "fields": []
  },
  {
    "name": "user_posted",
    "mode": "NULLABLE",
    "type": "STRING",
    "description": "",
    "fields": []
  },
  {
    "name": "community_members_num",
    "mode": "NULLABLE",
    "type": "INTEGER",
    "description": "",
    "fields": []
  },
  {
    "name": "input",
    "mode": "NULLABLE",
    "type": "RECORD",
    "description": "",
    "fields": [
      {
        "name": "url",
        "mode": "NULLABLE",
        "type": "STRING",
        "description": "",
        "fields": []
      }
    ]
  },
  {
    "name": "community_url",
    "mode": "NULLABLE",
    "type": "STRING",
    "description": "",
    "fields": []
  },
  {
    "name": "post_karma",
    "mode": "NULLABLE",
    "type": "INTEGER",
    "description": "",
    "fields": []
  },
  {
    "name": "community_rank",
    "mode": "NULLABLE",
    "type": "RECORD",
    "description": "",
    "fields": [
      {
        "name": "community_rank_value",
        "mode": "NULLABLE",
        "type": "STRING",
        "description": "",
        "fields": []
      },
      {
        "name": "community_rank_type",
        "mode": "NULLABLE",
        "type": "STRING",
        "description": "",
        "fields": []
      }
    ]
  },
  {
    "name": "comments",
    "mode": "REPEATED",
    "type": "RECORD",
    "description": "",
    "fields": [
      {
        "name": "replies",
        "mode": "REPEATED",
        "type": "RECORD",
        "description": "",
        "fields": [
          {
            "name": "num_replies",
            "mode": "NULLABLE",
            "type": "INTEGER",
            "description": "",
            "fields": []
          },
          {
            "name": "num_upvotes",
            "mode": "NULLABLE",
            "type": "INTEGER",
            "description": "",
            "fields": []
          },
          {
            "name": "date_of_reply",
            "mode": "NULLABLE",
            "type": "TIMESTAMP",
            "description": "",
            "fields": []
          },
          {
            "name": "user_url",
            "mode": "NULLABLE",
            "type": "STRING",
            "description": "",
            "fields": []
          },
          {
            "name": "reply",
            "mode": "NULLABLE",
            "type": "STRING",
            "description": "",
            "fields": []
          },
          {
            "name": "user_replying",
            "mode": "NULLABLE",
            "type": "STRING",
            "description": "",
            "fields": []
          }
        ]
      },
      {
        "name": "num_replies",
        "mode": "NULLABLE",
        "type": "INTEGER",
        "description": "",
        "fields": []
      },
      {
        "name": "user_commenting",
        "mode": "NULLABLE",
        "type": "STRING",
        "description": "",
        "fields": []
      },
      {
        "name": "num_upvotes",
        "mode": "NULLABLE",
        "type": "INTEGER",
        "description": "",
        "fields": []
      },
      {
        "name": "date_of_comment",
        "mode": "NULLABLE",
        "type": "TIMESTAMP",
        "description": "",
        "fields": []
      },
      {
        "name": "url",
        "mode": "NULLABLE",
        "type": "STRING",
        "description": "",
        "fields": []
      },
      {
        "name": "user_url",
        "mode": "NULLABLE",
        "type": "STRING",
        "description": "",
        "fields": []
      },
      {
        "name": "comment",
        "mode": "NULLABLE",
        "type": "STRING",
        "description": "",
        "fields": []
      }
    ]
  },
  {
    "name": "embedded_links",
    "mode": "REPEATED",
    "type": "STRING",
    "description": "",
    "fields": []
  },
  {
    "name": "community_description",
    "mode": "NULLABLE",
    "type": "STRING",
    "description": "",
    "fields": []
  },
  {
    "name": "related_posts",
    "mode": "REPEATED",
    "type": "RECORD",
    "description": "",
    "fields": [
      {
        "name": "num_comments",
        "mode": "NULLABLE",
        "type": "INTEGER",
        "description": "",
        "fields": []
      },
      {
        "name": "num_upvotes",
        "mode": "NULLABLE",
        "type": "INTEGER",
        "description": "",
        "fields": []
      },
      {
        "name": "thumbnail",
        "mode": "NULLABLE",
        "type": "STRING",
        "description": "",
        "fields": []
      },
      {
        "name": "url",
        "mode": "NULLABLE",
        "type": "STRING",
        "description": "",
        "fields": []
      },
      {
        "name": "title",
        "mode": "NULLABLE",
        "type": "STRING",
        "description": "",
        "fields": []
      },
      {
        "name": "community_url",
        "mode": "NULLABLE",
        "type": "STRING",
        "description": "",
        "fields": []
      },
      {
        "name": "community",
        "mode": "NULLABLE",
        "type": "STRING",
        "description": "",
        "fields": []
      }
    ]
  },
  {
    "name": "bio_description",
    "mode": "NULLABLE",
    "type": "STRING",
    "description": "",
    "fields": []
  },
  {
    "name": "date_posted",
    "mode": "NULLABLE",
    "type": "TIMESTAMP",
    "description": "",
    "fields": []
  },
  {
    "name": "videos",
    "mode": "REPEATED",
    "type": "STRING",
    "description": "",
    "fields": []
  },
  {
    "name": "num_upvotes",
    "mode": "NULLABLE",
    "type": "INTEGER",
    "description": "",
    "fields": []
  },
  {
    "name": "timestamp",
    "mode": "NULLABLE",
    "type": "TIMESTAMP",
    "description": "",
    "fields": []
  },
  {
    "name": "url",
    "mode": "NULLABLE",
    "type": "STRING",
    "description": "",
    "fields": []
  },
  {
    "name": "community_name",
    "mode": "NULLABLE",
    "type": "STRING",
    "description": "",
    "fields": []
  },
  {
    "name": "description",
    "mode": "NULLABLE",
    "type": "STRING",
    "description": "",
    "fields": []
  },
  {
    "name": "photos",
    "mode": "REPEATED",
    "type": "STRING",
    "description": "",
    "fields": []
  },
  {
    "name": "num_comments",
    "mode": "NULLABLE",
    "type": "INTEGER",
    "description": "",
    "fields": []
  },
  {
    "name": "tag",
    "mode": "NULLABLE",
    "type": "STRING",
    "description": "",
    "fields": []
  },
  {
    "name": "title",
    "mode": "NULLABLE",
    "type": "STRING",
    "description": "",
    "fields": []
  },
  {
    "name": "warning",
    "mode": "NULLABLE",
    "type": "STRING",
    "description": "",
    "fields": []
  },
  {
    "name": "post_id",
    "mode": "NULLABLE",
    "type": "STRING",
    "description": "",
    "fields": []
  },
  {
    "name": "snapshot_id",
    "mode": "REQUIRED",
    "type": "STRING",
    "description": "",
    "fields": []
  }
]
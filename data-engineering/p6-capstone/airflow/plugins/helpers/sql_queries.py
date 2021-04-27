class SqlQueries:
    """
    insert statements 
    """

    topics_table_insert = ("""
        insert into topics (topic_id, title, entry_count)
        select distinct topic_id, topic_title, entry_count
        from staging_topics
        where topic_id is not null
    """)

    authors_table_insert = ("""
        insert into authors (author_id, nick, total_entry_count, last_entry_date, is_deactivated, is_cursed,
                            follower_count, followings_count, picture_url, badges
                            )
        select distinct author_id, author_nick, entry_count,
                        to_timestamp(last_entry_date, 'yyyy-MM-ddTHH:mi:ss.MS'),
                        is_deactivated, is_cursed, follower_count, followings_count, picture, badges
        from staging_author_details
        where author_id is not null
    """)

    entry_update_dates_table_insert = ("""
        insert into entry_update_dates (date_updated, year, month, day, hour, week)
        with time as(
            select to_timestamp(last_updated, 'yyyy-MM-ddTHH:mi:ss.MS') as ts
            from staging_entries
            where last_updated is not null
        )
        select ts,
            extract(year from ts) as year,
            extract(month from ts) as month,
            extract(day from ts) as day,
            extract(hour from ts) as hour,
            extract(week from ts) as week
        from time
    """)

    entry_dates_table_insert = ("""
        insert into entry_dates (date_created, year, month, day, hour, week)
        with temp_time as(
            select to_timestamp(created, 'yyyy-MM-ddTHH:mi:ss.MS') as ts
            from staging_entries
            where created is not null
        )
        select ts,
            extract(year from ts) as year,
            extract(month from ts) as month,
            extract(day from ts) as day,
            extract(hour from ts) as hour,
            extract(week from ts) as week
        from temp_time
    """)

    entry_table_insert = ("""
        insert into entries (entry_id, date_updated, date_created, author_id, topic_id, favorite_count, comment_count)
        select distinct entry_id,
            to_timestamp(last_updated, 'yyyy-MM-ddTHH:mi:ss.MS'),
            to_timestamp(created, 'yyyy-MM-ddTHH:mi:ss.MS'),
            author_id,
            topic_id,
            favorite_count,
            comment_count
        from staging_entries
    """)

# ===============
# Staging tables
# ===============

staging_entries_table_drop = "drop table if exists staging_entries"
staging_author_details_table_drop = "drop table if exists staging_author_details"
staging_topics_table_drop = "drop table if exists staging_topics"
staging_authors_table_drop = "drop table if exists staging_authors"

staging_authors_table_create = ("""
CREATE TABLE IF NOT EXISTS public.staging_authors (
	author_id int,
	author_nick varchar(40)
);
""")

staging_topics_table_create = ("""
CREATE TABLE IF NOT EXISTS public.staging_topics (
	topic_id int,
	topic_title varchar(60),
	entry_count int,
	created varchar(27),
 	slug varchar
);
""")

staging_entries_table_create = ("""
CREATE TABLE IF NOT EXISTS public.staging_entries (
	entry_id int,
	author_nick varchar(40),
	author_id int,
	created varchar(27),
	last_updated varchar(27),
	is_favorite bool,
	favorite_count int,
	hidden bool,
	active bool,
	comment_count int,
	comment_summary text,
	avatar_url text,
	topic_id int,
	topic_title varchar(60),
	content text
);
""")

staging_author_details_table_create = ("""
CREATE TABLE IF NOT EXISTS public.staging_author_details (
	author_id integer,
	author_nick varchar(40),
	twitter_screen_name varchar(140),
	facbook_profile_url text,
	facebook_screen_name varchar(140),
	instagram_screen_name varchar(140),
	instagram_profile_url text,
	karma_name varchar(40),
	karma_value integer,
	entry_count integer,
	last_entry_date  varchar(25),
	is_corporate bool,
	is_deactivated bool,
	is_caylak bool,
	is_cursed bool,
	follower_count integer,
	followings_count integer,
	picture text,
	has_entry_on_seyler bool,
	badges text,
	favorite_entries text,
	favorited_entries text
);
""")

staging_create_table_queries = [
    staging_entries_table_create, staging_author_details_table_create,
    staging_topics_table_create, staging_authors_table_create
]

staging_drop_table_queries = [
    staging_entries_table_drop, staging_authors_table_drop,
    staging_topics_table_drop, staging_author_details_table_drop
]

# ==========================
# Fact and dimension tables
# ==========================


entries_table_drop = "drop table if exists entries cascade"
authors_table_drop = "drop table if exists authors cascade"
topics_table_drop = "drop table if exists topics cascade"
badges_table_drop = "drop table if exists badges cascade"
entry_dates_table_drop = "drop table if exists entry_dates cascade"
entry_update_dates_table_drop = "drop table if exists entry_update_dates cascade"
processed_entries_table_drop = "drop table if exists processed_entries cascade"
processed_topics_table_drop = "drop table if exists processed_topics cascade"

drop_table_queries = [
    entries_table_drop, authors_table_drop, topics_table_drop, badges_table_drop,
    entry_dates_table_drop, entry_update_dates_table_drop, processed_entries_table_drop, processed_topics_table_drop
]

# independent tables

entry_update_dates_table_create = ("""
CREATE TABLE IF NOT EXISTS public.entry_update_dates (
	date_updated timestamp primary key ,
    year int,
    month int,
    day int,
    hour int,
    week int
);
""")

entry_dates_table_create = ("""
CREATE TABLE IF NOT EXISTS public.entry_dates (
	date_created timestamp primary key ,
    year int,
    month int,
    day int,
    hour int,
    week int
);
""")

topics_table_create = ("""
CREATE TABLE IF NOT EXISTS public.topics (
	topic_id int primary key ,
	title varchar(60) not null,
	entry_count int
);
""")

badges_table_create = ("""
CREATE TABLE IF NOT EXISTS public.badges (
	badge_id int primary key,
	name varchar,
	description varchar
);
""")

# Dependent tables

processed_topics_table_create = ("""
CREATE TABLE IF NOT EXISTS public.processed_topics (
	topic_id int not null references topics(topic_id) primary key ,
	sentiment float,
	ner text,
	dates text
);
""")

authors_table_create = ("""
CREATE TABLE IF NOT EXISTS public.authors (
	author_id int primary key ,
	badges varchar,
	nick varchar(40) not null,
	total_entry_count int,
	last_entry_date timestamp,
	is_deactivated bool,
	is_cursed bool,
	follower_count int,
	followings_count int,
	picture_url varchar,
	favorite_entries text,
	favorited_entries text
);
""")

# Fact table
# not all the authors parsed yet, so I will remove the referance for now # todo
# not null references authors(author_id)
entries_table_create = ("""
CREATE TABLE IF NOT EXISTS public.entries (
	entry_id int primary key ,
	date_updated timestamp references entry_update_dates(date_updated),
	date_created timestamp not null references entry_dates(date_created),
	author_id int, 
	topic_id int not null references topics(topic_id),
	content text,
	favorite_count int,
	comment_count int
);
""")

processed_entries_table_create = ("""
CREATE TABLE IF NOT EXISTS public.processed_entries (
	entry_id int not null references entries(entry_id) primary key ,
	sentiment float,
	ner text,
	topic text
);
""")

create_table_queries = [
    entry_update_dates_table_create,
    entry_dates_table_create,
    topics_table_create,
    badges_table_create,
    processed_topics_table_create,
    authors_table_create,
    entries_table_create,
    processed_entries_table_create
]

# Insert queries

topics_table_insert = ("""
	insert into topics (topic_id, title, entry_count)
	select distinct topic_id, topic_title, entry_count
	from staging_topics
	where topic_id is not null
""")

authors_table_insert = ("""
	insert into authors (author_id, nick, total_entry_count, last_entry_date, is_deactivated, is_cursed,
						follower_count, followings_count, picture_url, favorite_entries, favorited_entries, badges
						)
	select distinct author_id, author_nick, entry_count,
					to_timestamp(last_entry_date, 'yyyy-MM-ddTHH:mi:ss.MS'),
					is_deactivated, is_cursed, follower_count, followings_count, picture, favorite_entries, favorited_entries, badges
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
		date_part ('year', ts) as year,
        date_part ('month', ts) as month,
        date_part ('day', ts) as day,
        date_part ('hour', ts) as hour,
        date_part ('week', ts) as week
	from time
	on conflict do nothing
""")

entry_dates_table_insert = ("""
    insert into entry_dates (date_created, year, month, day, hour, week)
	with temp_time as(
		select to_timestamp(created, 'yyyy-MM-ddTHH:mi:ss.MS') as ts
		from staging_entries
		where created is not null
	)
	select ts,
		date_part ('year', ts) as year,
        date_part ('month', ts) as month,
        date_part ('day', ts) as day,
        date_part ('hour', ts) as hour,
        date_part ('week', ts) as week
	from temp_time
	on conflict do nothing
""")

entry_table_insert = ("""
	insert into entries (entry_id, date_updated, date_created, author_id, topic_id, content, favorite_count, comment_count)
	select distinct entry_id,
	 	to_timestamp(last_updated, 'yyyy-MM-ddTHH:mi:ss.MS'),
	 	to_timestamp(created, 'yyyy-MM-ddTHH:mi:ss.MS'),
		author_id,
		topic_id,
		content,
		favorite_count,
		comment_count
	from staging_entries
""")

insert_table_queries = [
    topics_table_insert,
    authors_table_insert,
    entry_update_dates_table_insert,
    entry_dates_table_insert,
    entry_table_insert
]

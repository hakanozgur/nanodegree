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
	topic_title varchar(50),
	entry_count int
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
	topic_title varchar(50),
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

create_table_queries = [
    staging_entries_table_create, staging_author_details_table_create,
    staging_topics_table_create, staging_authors_table_create
]

drop_table_queries = [
    staging_entries_table_drop, staging_authors_table_drop,
    staging_topics_table_drop, staging_author_details_table_drop
]

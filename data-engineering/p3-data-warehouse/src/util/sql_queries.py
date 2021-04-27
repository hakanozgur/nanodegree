# %%
import configparser

# CONFIG

config = configparser.ConfigParser()
config.read(r'dwh.cfg')

config_log_data = config.get("S3", "LOG_DATA")
config_log_json = config.get("S3", "LOG_JSONPATH")
config_song_data = config.get("S3", "SONG_DATA")
config_arn = config.get("IAM_ROLE", "ARN")

# %%

# DROP TABLES

staging_events_table_drop = "drop table if exists staging_events"
staging_songs_table_drop = "drop table if exists staging_songs"
songplay_table_drop = "drop table if exists songplays"
user_table_drop = "drop table if exists users"
song_table_drop = "drop table if exists songs"
artist_table_drop = "drop table if exists artists"
time_table_drop = "drop table if exists time"

# CREATE TABLES
# {
#    "artist":"Mr Oizo",
#    "auth":"Logged In",
#    "firstName":"Kaylee",
#    "gender":"F",
#    "itemInSession":3,
#    "lastName":"Summers",
#    "length":144.03873,
#    "level":"free",
#    "location":"Phoenix-Mesa-Scottsdale, AZ",
#    "method":"PUT",
#    "page":"NextSong",
#    "registration":1540344794796.0,
#    "sessionId":139,
#    "song":"Flat 55",
#    "status":200,
#    "ts":1541106352796,
#    "userAgent":"\"Mozilla\/5.0 (Windows NT 6.1; WOW64) AppleWebKit\/537.36 (KHTML, like Gecko) Chrome\/35.0.19...."",
#    "userId":"8"
# } -> from 2018-11-01-events.json

staging_events_table_create = ("""
create table if not exists staging_events ( 
    artist varchar,
    auth varchar,
    firstName varchar,
    gender varchar,
    itemInSession int,
    lastName varchar,
    length float,
    level varchar,
    location varchar,
    method varchar,
    page varchar,
    registration varchar,
    sessionId int,
    song varchar,
    status int,
    ts timestamp, 
    userAgent varchar,
    userId int
    )
""")

# {
#    "artist_id":"AR73AIO1187B9AD57B",
#    "artist_latitude":37.77916,
#    "artist_location":"San Francisco, CA",
#    "artist_longitude":-122.42005,
#    "artist_name":"Western Addiction",
#    "duration":118.07302,
#    "num_songs":1,
#    "song_id":"SOQPWCR12A6D4FB2A3",
#    "title":"A Poor Recipe For Civic Cohesion",
#    "year":2005
# } -> from TRAAAAV128F421A322.json
staging_songs_table_create = ("""
create table if not exists staging_songs ( 
    song_id varchar primary key sortkey,
    artist_id varchar,
    artist_latitude float,
    artist_location varchar,
    artist_longitude float,
    artist_name varchar,
    duration float,
    num_songs int,
    title varchar,
    year int
    )
""")

songplay_table_create = ("""
create table if not exists songplays ( 
    songplay_id int identity(0, 1),
    start_time timestamp not null references time(start_time),
    user_id int not null references users(user_id),
    level varchar,
    song_id varchar references songs(song_id),
    artist_id varchar references artists(artist_id),
    session_id int,
    location varchar,
    user_agent varchar
    )
""")

user_table_create = ("""
create table if not exists users ( 
    user_id int primary key not null sortkey,
    first_name varchar,
    last_name varchar,
    gender char,
    level varchar
    ) diststyle all;
""")

song_table_create = ("""
create table if not exists songs ( 
    song_id varchar primary key sortkey,
    title varchar, 
    artist_id varchar not null references artists(artist_id),
    year int,
    duration float
    )
""")

artist_table_create = ("""
create table if not exists artists ( 
    artist_id varchar primary key sortkey,
    name varchar,
    location varchar,
    latitude float,
    longitude float
    )
""")

time_table_create = ("""
create table if not exists time ( 
    start_time timestamp primary key sortkey,
    hour int,
    day int,
    week int, 
    month int,
    year int,
    weekday int
    )
""")

# STAGING TABLES

staging_events_copy = ("""
    copy staging_events
    from {}
    iam_role {}
    json {}
    region 'us-west-2'
    timeformat as 'epochmillisecs'
""").format(config_log_data, config_arn, config_log_json)

staging_songs_copy = ("""
    copy staging_songs
    from {}
    iam_role {}
    region 'us-west-2'
    json 'auto'
""").format(config_song_data, config_arn)

# FINAL TABLES

user_table_insert = ("""
    insert into users (user_id, first_name, last_name, gender, level)
    select distinct 
        userId as user_id,
        firstName as first_name,
        lastName as last_name, 
        gender, level
    from staging_events
    where userId is not null
""")

song_table_insert = ("""
    insert into songs (song_id, title, artist_id, year, duration)
    select distinct song_id, title, artist_id, year, duration
    from staging_songs
    where song_id is not null 
""")

artist_table_insert = ("""
    insert into artists  (artist_id, name, location, latitude, longitude)
    select distinct artist_id, artist_name, artist_location , artist_latitude, artist_longitude
    from staging_songs
    where artist_id is not null
""")

time_table_insert = ("""
    insert into time (start_time, hour, day, week, month, year, weekday)
    select start_time,
        extract (hour from start_time) as hour,
        extract (day from start_time) as day,
        extract (week from start_time) as week,
        extract (month from start_time) as month,
        extract (year from start_time) as year,
        extract (dayofweek from start_time) as weekday
    from songplays;
""")

songplay_table_insert = ("""
    insert into songplays (start_time, user_id, level, session_id, location, user_agent, song_id, artist_id)
    select distinct  sevents.ts as start_time,
        sevents.userId as user_id,
        sevents.level as level,
        sevents.sessionId as session_id,
        sevents.location as location,
        sevents.userAgent as user_agent,
        ssongs.song_id as song_id,
        ssongs.artist_id as artist_id
    from staging_events as sevents, staging_songs as ssongs
    where sevents.song = ssongs.title and 
        sevents.artist = ssongs.artist_name and
        sevents.page='NextSong'
""")


# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, user_table_create, artist_table_create,
                        song_table_create, time_table_create, songplay_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop,
                      song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [user_table_insert, song_table_insert, artist_table_insert, songplay_table_insert,
                        time_table_insert]

# DROP TABLES

songplay_table_drop = "drop table if exists songplays"
user_table_drop = "drop table if exists users"
song_table_drop = "drop table if exists songs"
artist_table_drop = "drop table if exists artists"
time_table_drop = "drop table if exists time"

# CREATE TABLES

songplay_table_create = ("""
create table if not exists songplays ( 
    songplay_id serial primary key,
    start_time timestamp not null,
    user_id int not null,
    level varchar,
    song_id varchar,
    artist_id varchar,
    session_id int,
    location varchar,
    user_agent varchar,
    constraint fk_time
        foreign key (start_time)
            references time (start_time),
    constraint fk_user
        foreign key (user_id)
            references users (user_id),
    constraint fk_song
        foreign key (song_id)
            references songs (song_id),
    constraint fk_artist
        foreign key (artist_id)
            references artists (artist_id)
    );
""")

user_table_create = ("""
create table if not exists users ( 
    user_id int primary key not null,
    first_name varchar,
    last_name varchar,
    gender char,
    level varchar
    );
""")

song_table_create = ("""
create table if not exists songs ( 
    song_id varchar primary key,
    title varchar, 
    artist_id varchar not null,
    year int,
    duration float,
    constraint fk_artist
        foreign key (artist_id)
            references artists (artist_id)
    );
""")

artist_table_create = ("""
create table if not exists artists ( 
    artist_id varchar primary key,
    name varchar,
    location varchar,
    latitude float,
    longitude float
    );
""")

time_table_create = ("""
create table if not exists time ( 
    start_time timestamp primary key,
    hour int,
    day int,
    week int, 
    month int,
    year int,
    weekday int
    );
""")

# INSERT RECORDS

songplay_table_insert = ("""
insert into songplays (start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
values (%s,%s,%s,%s,%s,%s,%s,%s)
""")

user_table_insert = ("""
insert into users (user_id, first_name, last_name, gender, level)
values (%s,%s,%s,%s,%s) on conflict (user_id) do update set level=EXCLUDED.level
""")

song_table_insert = ("""
insert into songs (song_id, title, artist_id, year, duration)
values (%s,%s,%s,%s,%s) on conflict do nothing
""")

artist_table_insert = ("""
insert into artists  (artist_id, name, location, latitude, longitude)
values (%s,%s,%s,%s,%s) on conflict do nothing
""")


time_table_insert = ("""
insert into time (start_time, hour, day, week, month, year, weekday)
values (%s,%s,%s,%s,%s,%s,%s) on conflict do nothing
""")

# FIND SONGS

song_select = ("""
select so.song_id, so.artist_id from songs so
join artists ar on so.artist_id=ar.artist_id
where so.title = %s
and ar.name = %s
and so.duration = %s
""")

# QUERY LISTS

create_table_queries = [artist_table_create, user_table_create, song_table_create, time_table_create, songplay_table_create]
drop_table_queries = [songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
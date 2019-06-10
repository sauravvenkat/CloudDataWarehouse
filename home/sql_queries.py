import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs"
songplay_table_drop = "DROP TABLE IF EXISTS songplays"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS songs"
artist_table_drop = "DROP TABLE IF EXISTS artists"
time_table_drop = "DROP TABLE IF EXISTS time"

# CREATE TABLES

staging_events_table_create= ("""
CREATE TABLE IF NOT EXISTS staging_events
(
artist varchar(255),
auth varchar(255),
firstName varchar(255),
gender CHAR,
itemInSession integer,
lastName varchar(255),
length NUMERIC,
level varchar(255),
location text,
method text,
page text,
registration bigint,
sessionId integer,
song text,
status integer,
ts bigint,
userAgent text,
userId integer
)
""")

staging_songs_table_create = ("""
CREATE TABLE IF NOT EXISTS staging_songs
(
num_songs integer,
artist_id text,
artist_latitude NUMERIC,
artist_longitude NUMERIC,
artist_location text,
artist_name text,
song_id text,
title text,
duration numeric,
year integer
)
""")

songplay_table_create = ("""
CREATE TABLE IF NOT EXISTS songplays
(
songplay_id INT IDENTITY(0,1) PRIMARY KEY,
start_time timestamp NOT NULL,
user_id integer NOT NULL,
level text,
song_id text NOT NULL,
artist_id text NOT NULL,
session_id integer,
location text,
user_agent text
)
""")

user_table_create = ("""
CREATE TABLE IF NOT EXISTS users
(
user_id integer PRIMARY KEY,
first_name text,
last_name text,
gender char,
level text
)
""")

song_table_create = ("""
CREATE TABLE IF NOT EXISTS songs
(
song_id text PRIMARY KEY,
artist_id varchar NOT NULL,
title text,
year integer,
duration numeric
)
""")

artist_table_create = ("""
CREATE TABLE IF NOT EXISTS artist
(
artist_id text primary key,
name text,
location text,
latitude numeric,
longitude numeric
)
""")

time_table_create = ("""
CREATE TABLE IF NOT EXISTS time
(
start_time timestamp PRIMARY KEY,
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
COPY staging_events
FROM {}
iam_role {}
region 'us-west-2'
json 'auto';
""").format(config.get('S3', 'LOG_DATA'), config.get('IAM_ROLE', 'ARN'))



staging_songs_copy = ("""
COPY staging_songs
FROM {}
iam_role {}
region 'us-west-2'
json 'auto';
""").format(config.get('S3', 'SONG_DATA'), config.get('IAM_ROLE', 'ARN'))


# FINAL TABLES

songplay_table_insert = ("""
INSERT INTO songplays
(start_time, user_id, level, song_id, artist_id, session_id, location, user_agent ) 
SELECT '1970-01-01'::date + ts/1000 * interval '1 second', e.userId, e.level, s.song_id, s.artist_id, e.sessionId, e.location, e.useragent
FROM staging_events e
JOIN staging_songs s
ON e.artist = s.artist_name AND s.title = e.song
WHERE userId is not null;
""")

user_table_insert = ("""
INSERT INTO users (first_name, gender, last_name, level, user_id)
SELECT DISTINCT firstName, gender, lastName, level, userId
FROM staging_events
WHERE userId is not null;
""")

song_table_insert = ("""
INSERT INTO songs(duration, song_id, artist_id, title, year)
SELECT DISTINCT duration, song_id, artist_id, title, year
FROM staging_songs
""")


artist_table_insert = ("""
INSERT into artist (artist_id, latitude, location, longitude, name)
SELECT DISTINCT artist_id, artist_latitude, artist_location, artist_longitude, artist_name
FROM staging_songs;
""")

time_table_insert = ("""
INSERT INTO time (day, hour, month, start_time, week, weekday, year)
SELECT DISTINCT date_part(d, start_time), 
date_part(hr, start_time), 
date_part(month, start_time), 
start_time,
date_part(week, start_time),
date_part(dayofweek, start_time),
date_part(yr, start_time)
FROM songplays
""")

#QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]

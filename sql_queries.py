import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events;"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs;"
songplay_table_drop = "DROP TABLE IF EXISTS songplays;"
user_table_drop = "DROP TABLE IF EXISTS users;"
song_table_drop = "DROP TABLE IF EXISTS songs;"
artist_table_drop = "DROP TABLE IF EXISTS artists;"
time_table_drop = "DROP TABLE IF EXISTS time;"

# CREATE TABLES

staging_events_table_create= ("""
CREATE TABLE staging_events (
    "artist" varchar,
    "auth" varchar,
    "firstname" varchar,
    "gender" varchar,
    "iteminSession" smallint,
    "lastname" varchar,
    "length" NUMERIC,
    "level" varchar,
    "location" text,
    "method" varchar,
    "page" varchar,
    "regisration" bigint,
    "sessionId" INTEGER,
    "song" varchar,
    "status" INTEGER,
    "ts" timestamp,
    "userAgent" text,
    "userId" int
);

""")

staging_songs_table_create = ("""

CREATE TABLE staging_songs (
    "num_songs" INTEGER,
    "artist_id" text,
    "artist_latitude" varchar,
    "artist_longitude" varchar,
    "artist_location" text,
    "artist_name" varchar,
    "song_id" text,
    "title" varchar,
    "duration" NUMERIC,
    "year" int
);



""")

songplay_table_create = ("""
CREATE TABLE IF NOT EXISTS songplays (
songplay_id INTEGER IDENTITY(1, 1) PRIMARY KEY , 
start_time timestamp, 
user_id text, 
level varchar , 
song_id text, 
artist_id text, 
session_id text, 
location text, 
user_agent text
);

""")

user_table_create = ("""
CREATE TABLE IF NOT EXISTS users (
user_id text PRIMARY KEY, 
first_name varchar,
last_name varchar, 
gender varchar, 
level varchar sortkey
)
diststyle all;
""")

song_table_create = ("""
CREATE TABLE IF NOT EXISTS songs (
song_id text PRIMARY KEY,
title varchar,
artist_id text, 
year int sortkey,
duration decimal
)
diststyle all;

""")

artist_table_create = ("""
CREATE TABLE IF NOT EXISTS artists (
artist_id text PRIMARY KEY, 
name varchar, 
location varchar, 
latitude decimal, 
longitude decimal
)
diststyle all;
""")

time_table_create = ("""
CREATE TABLE IF NOT EXISTS time (
start_time timestamp PRIMARY KEY, 
hour int, 
day int, 
week int,
month int, 
year int, 
weekday int
)
diststyle all;
""")

# STAGING TABLES

staging_events_copy = ("""
 copy staging_events from {}
    
    credentials 'aws_iam_role={}'
    
    compupdate off region 'us-west-2'
    TIMEFORMAT AS 'epochmillisecs'
    format as json {};
""").format(config['S3']['LOG_DATA'],config['IAM_ROLE']['ARN'],config['S3']['LOG_JSONPATH'])

staging_songs_copy = ("""
copy staging_songs from {}
    credentials 'aws_iam_role={}'
    json 'auto'
    TIMEFORMAT AS 'epochmillisecs'
    compupdate off 
    region 'us-west-2';
""").format(config['S3']['SONG_DATA'],config['IAM_ROLE']['ARN'])

# FINAL TABLES

songplay_table_insert = ("""
INSERT INTO songplays(start_time,user_id,level,song_id,
artist_id,session_id,location, 
user_agent)
SELECT  events.ts,
        events.userId,
        events.level,
        stagesong.song_id,
        stagesong.artist_id,
        events.sessionId,
        events.location,
        events.userAgent
FROM
staging_events as events
JOIN 
staging_songs as stagesong
ON
events.artist=stagesong.artist_name
WHERE events.page = 'NextSong'
""")

user_table_insert = ("""
INSERT INTO users(user_id,
first_name,last_name,gender,level)

SELECT DISTINCT userId, firstname, lastname, gender, level
  FROM staging_events
  WHERE page='NextSong'
""")

song_table_insert = ("""
INSERT INTO songs (song_id,title,
artist_id,year,duration)

SELECT DISTINCT song_id,title,artist_id,year,duration
FROM staging_songs;
""")

artist_table_insert = ("""
INSERT INTO artists (artist_id, name,
location, latitude, longitude)

SELECT DISTINCT artist_id, artist_name,
artist_location, artist_latitude, artist_longitude
FROM staging_songs;

""")

time_table_insert = ("""
INSERT INTO time (start_time,hour,day,week,month,year,weekday)
SELECT DISTINCT ts as start_time,
EXTRACT(HOUR FROM ts) AS hour,
EXTRACT(DAY FROM ts) AS day,
EXTRACT(WEEK FROM ts) AS week,
EXTRACT(MONTH FROM ts) AS month,
EXTRACT(YEAR FROM ts) AS year,
EXTRACT(WEEKDAY FROM ts) AS weekday

FROM staging_events
WHERE page='NextSong'
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]

import configparser
import os


def get_app_file_path(file):
    """Return the absolute path of the app's files. They should be in the same folder as this py file."""
    folder,_ = os.path.split(__file__)
    file_path = os.path.join(folder,file)
    return file_path
# CONFIG
config = configparser.ConfigParser()
file_name = 'dwh.cfg'
file = get_app_file_path(file_name)
print(file)
config.read(file)



# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events;"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs;"
songplay_table_drop = "DROP TABLE IF EXISTS songplays;"
user_table_drop = "DROP TABLE IF EXISTS users;"
song_table_drop = "DROP TABLE IF EXISTS songs;"
artist_table_drop = "DROP TABLE IF EXISTS artists;"
time_table_drop = "DROP TABLE IF EXISTS time;"


ARN             = config.get('IAM_ROLE', 'ARN')
LOG_DATA        = config.get('S3', 'LOG_DATA')
LOG_JSONPATH    = config.get('S3', 'LOG_JSONPATH')
SONG_DATA       = config.get('S3', 'SONG_DATA')


# CREATE TABLES

staging_events_table_create= ("""
    CREATE SCHEMA IF NOT EXISTS nodist;
    SET search_path TO nodist;
    CREATE TABLE IF NOT EXISTS staging_events(
        event_id INT IDENTITY(0,1) NOT NULL,
        artist_name VARCHAR(255)  NULL,
        auth VARCHAR(50) NULL,
        user_first_name VARCHAR(255) NULL,
        user_gender VARCHAR(10) NULL,
        item_in_session INTEGER NULL,
        user_last_name VARCHAR(255) NULL,
        song_length FLOAT NULL,
        user_level VARCHAR(50) NULL,
        location VARCHAR(255) NULL,
        method VARCHAR(25) NULL,
        page VARCHAR(50) NULL,
        registration VARCHAR(50) NULL,
        session_id	BIGINT NULL SORTKEY DISTKEY,
        song_title VARCHAR(255) NULL,
        status INTEGER NULL,
        ts TIMESTAMP NULL,
        user_agent TEXT NULL,
        user_id INTEGER NULL
    );

""")

staging_songs_table_create = ("""
    CREATE TABLE staging_songs(
        song_id VARCHAR(100) PRIMARY KEY,
        num_songs INTEGER,
        artist_id VARCHAR(100),
        artist_latitude DOUBLE PRECISION,
        artist_longitude DOUBLE PRECISION,
        artist_location VARCHAR(255),
        artist_name VARCHAR(255),
        title VARCHAR(255),
        duration DOUBLE PRECISION,
        year DOUBLE PRECISION
    );
""")

songplay_table_create = ("""
    CREATE TABLE songplays(
        songplay_id INT IDENTITY(0,1) PRIMARY KEY,
        start_time TIMESTAMP SORTKEY,
        user_id INTEGER NOT NULL DISTKEY,
        level VARCHAR(50),
        song_id VARCHAR(100),
        artist_id VARCHAR(100),
        session_id BIGINT,
        location VARCHAR(255),
        user_agent TEXT
        )
    DISTSTYLE KEY;
""")

user_table_create = ("""
    CREATE TABLE users(
        user_id INTEGER NOT NULL SORTKEY,
        first_name VARCHAR(255) NULL ,
        last_name VARCHAR(255),
        gender VARCHAR(10),
        level VARCHAR(50)
        );
""")

song_table_create = ("""
    CREATE TABLE songs(
        song_id VARCHAR(100) PRIMARY KEY,
        title VARCHAR(255),
        artist_id VARCHAR(100) NOT NULL DISTKEY,
        year INTEGER,
        duration DOUBLE PRECISION
        );
""")

artist_table_create = ("""
    CREATE TABLE artists(
        artist_id VARCHAR(100) PRIMARY KEY DISTKEY,
        name VARCHAR(255),
        location VARCHAR(255),
        latitude DOUBLE PRECISION,
        longitude DOUBLE PRECISION
        );
""")

time_table_create = ("""
    CREATE TABLE time(
        start_time TIMESTAMP PRIMARY KEY,
        hour INTEGER,
        day INTEGER,
        week INTEGER,
        month INTEGER,
        year INTEGER,
        weekday INTEGER
        );
""")

# STAGING TABLES

staging_events_copy = ("""
    COPY staging_events FROM {}
    CREDENTIALS 'aws_iam_role= {}'
    FORMAT as json {}
    STATUPDATE ON
    region 'us-west-2';
""").format(LOG_DATA, ARN, LOG_JSONPATH)

staging_songs_copy = ("""
    COPY staging_songs FROM {}
    CREDENTIALS 'aws_iam_role= {}'
    FORMAT as json 'auto'
    ACCEPTINVCHARS AS '^'
    STATUPDATE ON
    region 'us-west-2';
""").format(SONG_DATA, ARN)
# FINAL TABLES

songplay_table_insert = ("""
    INSERT INTO songplays(start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
    SELECT
        TIMESTAMP 'epoch' + (se.ts/1000) * INTERVAL '1 second' AS start_time,
        se.user_id,
        se.user_level,
        ss.song_id,
        ss.artist_id,
        se.session_id,
        se.location,
        se.user_agent
    FROM staging_events se, staging_songs ss
    WHERE se.page = 'NextSong'
    AND se.song_title = ss.title
    AND se.artist_name = ss.artist_name
    AND se.song_length = ss.duration
""")

user_table_insert = ("""
    INSERT INTO users(user_id, first_name, last_name, gender, level)
    SELECT DISTINCT
        user_id, 
        user_first_name, 
        user_last_name, 
        user_gender, 
        user_level
    FROM staging_events
    WHERE page = 'NextSong'
""")

song_table_insert = ("""
    INSERT INTO songs (song_id, title, artist_id, year, duration) 
    SELECT DISTINCT
        song_id,
        title,
        artist_id,
        year,
        duration
    FROM staging_songs
    WHERE song_id IS NOT NULL
""")

artist_table_insert = ("""
    INSERT INTO artists (artist_id, name, location, latitude, longitude)
    SELECT DISTINCT
        artist_id,
        artist_name,
        artist_location,
        artist_latitude,
        artist_longitude
    FROM staging_songs
    WHERE artist_id IS NOT NULL
""")

time_table_insert = ("""
    INSERT INTO time
    SELECT DISTINCT,
        TIMESTAMP 'epoch' + (ts/1000) * INTERVAL '1 second' as start_time,
        EXTRACT(HOUR FROM start_time) AS hour,
        EXTRACT(DAY FROM start_time) AS day,
        EXTRACT(WEEK FROM  start_time) AS week,
        EXTRACT(MONTH FROM start_time) AS month,
        EXTRACT(YEAR FROM start_time) AS year,
        to_char(start_time, 'Day') AS weekday
    FROM staging_events
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]

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
config.read(file_name)



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
REGION          = config.get('CLUSTER', 'DB_REGION')


# CREATE TABLES

staging_events_table_create= ("""
    CREATE TABLE IF NOT EXISTS staging_events(
    artist          VARCHAR(255),
    auth            VARCHAR(50),
    firstName       VARCHAR(50),
    gender          VARCHAR(2),
    itemInSession   INTEGER,
    lastName        VARCHAR(50),
    length          FLOAT,
    level           VARCHAR(10),
    location        VARCHAR(255),
    method          VARCHAR(20),
    page            VARCHAR(30),
    registration    FLOAT,
    sessionId       INTEGER,
    song            VARCHAR(255),
    status          INTEGER,
    ts              BIGINT,
    userAgent       VARCHAR(255),
    userId          INTEGER
    );

""")

staging_songs_table_create = ("""
    CREATE TABLE IF NOT EXISTS staging_songs(
    song_id         VARCHAR(100),
    num_songs       INTEGER,
    artist_id       VARCHAR(100),
    artist_latitude  FLOAT,
    artist_longitude FLOAT,
    artist_location VARCHAR(255),
    artist_name     VARCHAR(255),
    title           VARCHAR(255),
    duration        FLOAT,
    year            INTEGER
    );
""")

songplay_table_create = ("""
    CREATE TABLE IF NOT EXISTS songplays(
    songplay_id     INTEGER IDENTITY (1, 1) PRIMARY KEY ,
    start_time      TIMESTAMP,
    user_id         INTEGER,
    level           VARCHAR(10),
    song_id         VARCHAR(100),
    artist_id       VARCHAR(100),
    session_id      INTEGER,
    location        VARCHAR(255),
    user_agent      VARCHAR(255)
)
DISTSTYLE KEY
DISTKEY ( start_time )
SORTKEY ( start_time );
""")

user_table_create = ("""
    CREATE TABLE IF NOT EXISTS users(
    userId          INTEGER PRIMARY KEY,
    firsname        VARCHAR(50),
    lastname        VARCHAR(50),
    gender          VARCHAR(2) ENCODE BYTEDICT,
    level           VARCHAR(10) ENCODE BYTEDICT
)
SORTKEY (userId);
""")

song_table_create = ("""
    CREATE TABLE IF NOT EXISTS songs(
    song_id         VARCHAR PRIMARY KEY,
    title           VARCHAR(255),
    artist_id       VARCHAR(100),
    year            INTEGER ENCODE BYTEDICT,
    duration        FLOAT
)
SORTKEY (song_id);
""")

artist_table_create = ("""
    CREATE TABLE IF NOT EXISTS artists(
    artist_id       VARCHAR PRIMARY KEY ,
    name            VARCHAR(255),
    location        VARCHAR(255),
    latitude        FLOAT,
    longitude       FLOAT
)
SORTKEY (artist_id);
""")

time_table_create = ("""
    CREATE TABLE IF NOT EXISTS time(
    start_time      TIMESTAMP PRIMARY KEY ,
    hour            INTEGER,
    day             INTEGER,
    week            INTEGER,
    month           INTEGER,
    year            INTEGER ENCODE BYTEDICT ,
    weekday         VARCHAR(9) ENCODE BYTEDICT
)
DISTSTYLE KEY
DISTKEY ( start_time )
SORTKEY (start_time);
""")
# STAGING TABLES

staging_events_copy = ("""
    COPY staging_events
    FROM {}
    credentials 'aws_iam_role={}'
    format as json {}
    STATUPDATE ON
    region '{}';
""").format(LOG_DATA,ARN,LOG_JSONPATH,REGION)

staging_songs_copy = ("""
    COPY staging_songs
    FROM {}
    credentials 'aws_iam_role={}'
    format as json 'auto'
    ACCEPTINVCHARS AS '^'
    STATUPDATE ON
    region '{}';
""").format(SONG_DATA,ARN,REGION)

# FINAL TABLES


songplay_table_insert = ("""
INSERT INTO songplays (START_TIME, USER_ID, LEVEL, SONG_ID, ARTIST_ID, SESSION_ID, LOCATION, USER_AGENT)
SELECT DISTINCT
       TIMESTAMP 'epoch' + (se.ts / 1000) * INTERVAL '1 second' as start_time,
            se.userId,
            se.level,
            ss.song_id,
            ss.artist_id,
            se.sessionId,
            se.location,
            se.userAgent
        FROM staging_songs ss
        INNER JOIN staging_events se
        ON (ss.title = se.song AND se.artist = ss.artist_name)
        AND se.page = 'NextSong';
""")

user_table_insert = ("""
        INSERT INTO users
        SELECT DISTINCT userId,
                        firstName,
                        lastName,
                        gender,
                        level
        FROM staging_events
        WHERE userId IS NOT NULL
        AND page = 'NextSong';
""")

song_table_insert = ("""
        INSERT INTO songs
        SELECT DISTINCT song_id,
                        title,
                        artist_id,
                        year,
                        duration
        FROM staging_songs
        WHERE song_id IS NOT NULL;
""")

artist_table_insert = ("""
        INSERT INTO artists
        SELECT DISTINCT artist_id,
                        artist_name,
                        artist_location,
                        artist_latitude,
                        artist_longitude
        FROM staging_songs;
""")
time_table_insert = ("""
        INSERT INTO time
        SELECT DISTINCT
            TIMESTAMP 'epoch' + (ts/1000) * INTERVAL '1 second' as start_time,
            EXTRACT(HOUR FROM start_time) AS hour,
            EXTRACT(DAY FROM start_time) AS day,
            EXTRACT(WEEKS FROM start_time) AS week,
            EXTRACT(MONTH FROM start_time) AS month,
            EXTRACT(YEAR FROM start_time) AS year,
            to_char(start_time, 'Day') AS weekday
        FROM staging_events;
""")
# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]

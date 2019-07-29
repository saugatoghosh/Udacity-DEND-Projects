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
    CREATE TABLE IF NOT EXISTS staging_events(
        artist          varchar,
        auth            text,
        first_name      varchar,
        gender          char,
        itemInSession   int,
        last_name       varchar,
        length          numeric,
        level           text,
        location        varchar,
        method          varchar,
        page            text,
        registration    numeric,
        session_id      int,
        song            varchar,
        status          int,
        ts              bigint,
        user_agent      text,
        user_id         int);
""")

staging_songs_table_create = ("""
    CREATE TABLE IF NOT EXISTS staging_songs (
       num_songs           int,
       artist_id           varchar,
       artist_latitude     real,
       artist_longitude    real,
       artist_location     varchar,
       artist_name         varchar,
       song_id             varchar,
       title               varchar,
       duration            numeric,
       year                int);
""")

songplay_table_create = ("""
    CREATE TABLE IF NOT EXISTS songplays (
        songplay_id        int IDENTITY(0,1) PRIMARY KEY sortkey distkey, 
        start_time         timestamp NOT NULL, 
        user_id            int NOT NULL, 
        level              text, 
        song_id            varchar, 
        artist_id          varchar, 
        session_id         int, 
        location           varchar, 
        user_agent         varchar);
""")

user_table_create = ("""
    CREATE TABLE IF NOT EXISTS users (
        user_id           int PRIMARY KEY sortkey, 
        first_name        varchar, 
        last_name         varchar, 
        gender            char, 
        level             text)
    diststyle all;
""")

song_table_create = ("""
    CREATE TABLE IF NOT EXISTS songs (
        song_id          varchar PRIMARY KEY sortkey, 
        title            varchar, 
        artist_id        varchar, 
        year             int, 
        duration         numeric)
    diststyle all;
""")

artist_table_create = ("""
    CREATE TABLE IF NOT EXISTS artists (
        artist_id        varchar PRIMARY KEY sortkey, 
        name             varchar, 
        location         varchar, 
        latitude         real, 
        longitude        real)
    diststyle all;
""")

time_table_create = ("""
    CREATE TABLE IF NOT EXISTS time (
        start_time      timestamp PRIMARY KEY sortkey, 
        hour            int, 
        day             int, 
        week            int, 
        month           int, 
        year            int, 
        weekday         int)
    diststyle all;
""")

# STAGING TABLES

staging_events_copy = ("""
    COPY     staging_events
    FROM     {}
    IAM_ROLE {}
    FORMAT AS JSON {};
""").format(config.get("S3","LOG_DATA"), 
            config.get("IAM_ROLE","ARN"), 
            config.get("S3","LOG_JSONPATH")
           )

staging_songs_copy = ("""
    COPY     staging_songs
    FROM     {}
    IAM_ROLE {}
    FORMAT AS JSON 'auto';  
""").format(config.get("S3","SONG_DATA"), 
            config.get("IAM_ROLE","ARN")
           )

# FINAL TABLES

songplay_table_insert = ("""
    INSERT into songplays(start_time, user_id, level, song_id, artist_id,\
    session_id, location, user_agent)
    SELECT DISTINCT TIMESTAMP 'epoch' + ts/1000*INTERVAL '1 Second' AS start_time, \
    se.user_id, se.level, ss.song_id, ss.artist_id, se.session_id, se.location, se.user_agent
    FROM staging_events se
    JOIN staging_songs ss ON (se.song = ss.title AND se.artist = ss.artist_name AND se.length = ss.duration );
""")

user_table_insert = ("""
    INSERT into users(user_id, first_name, last_name, gender, level)
    SELECT DISTINCT user_id, first_name, last_name, gender, level
    FROM staging_events;
""")

song_table_insert = ("""
    INSERT into songs(song_id, title, artist_id, year, duration)
    SELECT DISTINCT song_id, title, artist_id, year, duration
    FROM staging_songs;
""")

artist_table_insert = ("""
    INSERT into artists(artist_id, name, location, latitude, longitude)
    SELECT DISTINCT artist_id, artist_name, artist_location, artist_latitude, artist_longitude
    FROM staging_songs;
""")

time_table_insert = ("""
    INSERT into time(start_time, hour, day, week, month, year, weekday)
    SELECT DISTINCT TIMESTAMP 'epoch' + ts/1000*INTERVAL '1 Second' AS start_time,
    EXTRACT(hr FROM start_time) AS hour,
    EXTRACT(d FROM start_time) AS day,
    EXTRACT(w FROM start_time) AS week,
    EXTRACT(mon FROM start_time) AS month,
    EXTRACT(y FROM start_time) AS year,
    EXTRACT(dow FROM start_time) AS weekday
    FROM staging_events;
""")



# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]

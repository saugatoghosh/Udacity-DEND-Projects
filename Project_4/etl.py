import findspark
findspark.init('/home/ubuntu/spark-2.4.3-bin-hadoop2.7')

import configparser
import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col, monotonically_increasing_id, to_timestamp
from pyspark.sql.types import IntegerType
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format


config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config['AWS']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS']['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    """
    creates a spark session
    """

    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    """
    extracts song files, transforms them into songs and artists tables and \
    loads them as parquet tables
    """
    # get filepath to song data file
    song_data = os.path.join(input_data, 'song_data/*/*/*/*.json')
    
    # read song data file
    df = spark.read.json(song_data)

    # extract columns to create songs table
    songs_table = df.select(col("song_id"), col("title"), col("artist_id"), col("year"), col("duration")).dropDuplicates(['song_id'])
    
    # write songs table to parquet files partitioned by year and artist
    songs_table.write.partitionBy("year", "artist_id").parquet(os.path.join(output_data, "songs"), "overwrite")
    print('songs table written!')

    # extract columns to create artists table
    artists_table = df.select(col("artist_id"), col("artist_name").alias("name"), col("artist_location").alias("location"), \
                             col("artist_latitude").alias("latitude"), col("artist_longitude").alias("longitude")) \
                             .dropDuplicates(["artist_id"])
    
    # write artists table to parquet files
    artists_table.write.parquet(os.path.join(output_data, "artists"), "overwrite")
    print('artists table written!')


def process_log_data(spark, input_data, output_data):
    """
    extracts log files, transforms them into songplays, time and users tables \
    and loads them as parquet tables
    """
    # get filepath to log data file
    log_data = os.path.join(input_data, 'log_data/*/*/*.json')

    # read log data file
    df = spark.read.json(log_data)
    
    # filter by actions for song plays
    df = df.filter(df.page == 'NextSong')

    # extract columns for users table    
    users_table = df.select(col("userId").alias("user_id"), col("firstName").alias("first_name"), col("lastName").alias("last_name"), \
                            col("gender"), col("level")).dropDuplicates(["user_id"])
    
    # write users table to parquet files
    users_table.write.parquet(os.path.join(output_data, "users"), "overwrite")
    print('users table written!')

    # create timestamp column from original timestamp column
    get_timestamp = udf(lambda x:datetime.datetime.fromtimestamp(x/1000.0).strftime('%Y-%m-%d %H:%M:%S'))
    df = df.withColumn("start_time", to_timestamp(get_timestamp(df.ts)))
    
    # create hour, day, week, month, year, weekday columns from timestamp column
    get_weekday = udf(lambda x: datetime.datetime.fromtimestamp(x/1000.0).strftime('%w'))
    df = df.withColumn("hour", hour(col("start_time"))) \
                .withColumn("day", dayofmonth(col("start_time"))) \
                .withColumn("week", weekofyear(col("start_time"))) \
                .withColumn("month", month(col("start_time"))) \
                .withColumn("year", year(col("start_time"))) \
                .withColumn("weekday", get_weekday(df.ts).cast(IntegerType()))
    
    # extract columns to create time table
    time_table = df.select(col("start_time"), col("hour"), col("day"), \
                           col("week"), col("month"), col("year"), \
                           col("weekday")).dropDuplicates(["start_time"])
    # write time table to parquet files partitioned by year and month
    time_table.write.partitionBy("year", "month").parquet(os.path.join(output_data, "time"), "overwrite")
    print('time table written!')

    # read in song data to use for songplays table
    song_data = os.path.join(input_data, 'song_data/*/*/*/*.json')
    song_df = spark.read.json(song_data)

    # extract columns from joined song and log datasets to create songplays table 
    songplays_table = df.join(song_df, (df.song == song_df.title) & (df.artist == song_df.artist_name) \
                             & (df.length == song_df.duration), 'left_outer').select(df.start_time, \
                             df.userId.alias("user_id"), df.level, song_df.song_id, song_df.artist_id, \
                             df.sessionId.alias("session_id"), df.location, df.userAgent.alias("user_agent"), \
                             df.year, df.month) \
                             .withColumn("songplay_id", monotonically_increasing_id())
                             
                             

    # write songplays table to parquet files partitioned by year and month
    songplays_table.write.partitionBy("year", "month").parquet(os.path.join(output_data, "songplays"), "overwrite")
    print('songplays table written!')


def main():
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = "s3a://sougata-dend"
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()

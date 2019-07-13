# Project Overview

A music streaming startup, Sparkify, has grown their user base and song database even more and want to move their data warehouse to a 
data lake. Their data resides in S3, in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata 
on the songs in their app.

The objective of the project is to build an ETL pipeline that extracts their data from S3, processes them using Spark, 
and loads the data back into S3 as a set of dimensional tables. This will allow their analytics team to continue finding insights in
what songs their users are listening to. The subtasks in the etl pipeline include :

- loading data from S3
- processing the data into analytics tables using Spark
- loading the data back into S3 as as set of dimensional tables.

# Source Datasets

- **Song Dataset** - Subset of data from the [Million Song](https://labrosa.ee.columbia.edu/millionsong/) dataset, each file in json
format and containing metadata about a song and artist.

- **Log Dataset** - Log files in json format based on the songs in the dataset above which simulate app activity logs from a music 
streaming app based on specified configurations.

# Database schema 

The database schema consists of the following fact and dimension tables arranged as a **star schema**. 

- **Fact Table**
  - songplays : records in log data associated with song plays
 
- **Dimension Tables**
  - **users** : users in the app
  - **songs** : songs in music database
  - **artists** : artists in music database
  - **time** : timestamps of records in songplays broken down into specific units

The benefits of using a star schema is that the tables can be denormalized, queries simplified and aggregations made faster

# Files in ETL pipeline

- dl.cfg : contains aws credentials
- etl.py : reads and processes files from song  data and log data and loads into tables
- test.ipynb :  displays the first few rows of each table to check database. Can also include other analytical queries.

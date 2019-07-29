# Project Overview

A music streaming startup, Sparkify, has grown their user base and song database and wants to move  their processes and data onto the cloud. 

Their data resides in S3, in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata 

on the songs in their app.



The objective of the project is to extract the data and transform it into a set of dimensional tables for their analytics team to 

continue finding insights in what songs their users are listening to. 


The subtasks include :

- creating a database schema
- building an ETL pipeline that extracts data from S3, stages them in Redshift, 
  and transforms data into a set of dimensional tables in line with the schema
  

# Source Datasets

- **Song Dataset** - Subset of data from the [Million Song](https://labrosa.ee.columbia.edu/millionsong/) dataset, 
  each file containing metadata about a song and artist.

- **Log Dataset** - Log files based on the songs in the dataset above which simulate app activity logs from
  a music streaming app based on specified configurations.


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


# ETL Pipeline

 The etl pipeline consists of extracting the song and log data from S3 and staging them in two intermediate tables - **staging_songs**
 
 and **staging_events**. The advantage of using this process that the data can be imported into the redshift database as is and then
 
 inserted as required into the fact and dimension tables using simple sql queries.
 
 
# Files in ETL pipeline

- create_tables.py : creates database and tables (after dropping tables first)
- sql_queries.py : contains create table , drop table, and insert queries and is imported into 
  create_tables.py and etl.py
- etl.py : reads and processes files from song  data and log data and loads into tables

# Test Analytical Queries

```
    %sql SELECT COUNT(*) FROM songs;
```

```
    %sql SELECT * FROM users LIMIT 5;
```

```
    %sql SELECT * FROM songplays WHERE song_id is not null;
```



# Project Overview

A startup called Sparkify wants to analyze the data they've been collecting on songs and user activity on their new music streaming app. 
The analytics team is particularly interested in understanding what songs users are listening to. Currently, they don't have an easy way
to query their data, which resides in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata
on the songs in their app.

The objective of the project is to create a Postgres database with tables designed to optimize queries on song play analysis. The subtasks
include :

- creating a database schema
- building an ETL pipeline using Python

# Source Datasets

- **Song Dataset** - Subset of data from the [Million Song](https://labrosa.ee.columbia.edu/millionsong/) dataset, each file containing metadata about a song and artist.

- **Log Dataset** - Log files based on the songs in the dataset above which simulate app activity logs from a music streaming app based on specified configurations.

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

- create_tables.py : creates database and tables (after dropping tables first)
- sql_queries.py : contains create table , drop table, insert and select queries and is imported into create_tables.py, etl.ipynb and etl.py
- etl.ipynb : reads and processes a single file from song data and log data and loads into tables
- etl.py : reads and processes files from song  data and log data and loads into tables
- test.ipynb :  displays the first few rows of each table to check database. Can also include other analytical queries.




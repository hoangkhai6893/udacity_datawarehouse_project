# Project Data Warehouse
 ## Introduction
    A music streaming startup, Sparkify, has grown their user base and song database and want to move their processes and data onto the cloud. Their data resides in S3, in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app.

    As their data engineer, you are tasked with building an ETL pipeline that extracts their data from S3, stages them in Redshift, and transforms data into a set of dimensional tables for their analytics team to continue finding insights into what songs their users are listening to. You'll be able to test your database and ETL pipeline by running queries given to you by the analytics team from Sparkify and compare your results with their expected results.
 ## Project Description
    In this project, The application of Data warehouse and AWS will be running to build an ETL Pipeline for a database hosted on Redshift will need to load data from S3 to staging tables on Redshift and execute SQL.
 ## Project Dataset
    Dataset used in this project is provided by two public S3 buckets.
    One bucket contains info about songs and artists. Song data: s3://udacity-dend/song_data
    The second bucket contains info action of users.Log data: s3://udacity-dend/log_data
##  Database Schema
    Staging_table
        -staging_songs: info about songs and artists
        -staging_events: actions done by users (which song are listening, etc.. )
    Fact Table:
        -songplays: records in event data associated with song plays
    Dimension Table:
        -users: users in the application
        -songs: songs in music playlist dataset
        -artists: artists in the application
        -time: timestamps of records in songplays broken down into specific units.
## Files: 
- create_tables.py: Drop old tables and create new tables by using SQL script
- dwh.cfg: Configurations IAM,AWS,S3 and RedShift
- etl.py: Copy data to staging tables and insert data to fact and dimension tables.
- sql_queries.py: This file contains variables with SQL statement  to CREATE, DROP, COPY and INSERT statement.
- datawarehouse.ipynb: the jupyter python file to create IAM role,RedShift and open TCP port. The result is DWH_ENDPOINT and DWH_ROLE_ARN
- requirements.txt: Contain python pip library need for project.
## Configurations and Setup
    Step 1: Create a new IAM user in AWS, give it AdministratorAccess and Attach policies 
    Step 2: Use access key and secret key to complete the dwh.cfg file. 
    Step 3: Create an IAM Role the make Redshift can access the S3 Bucket.
    Step 4: Create a RedShift Cluster,get the DWH_ENDPOINT and DWH_ROLE_ARN
## ETL Pipeline
    Step 1: Create tables to get data from s3
    Step 2: Load data from S3 to staging tables in the RedShift.
    Step 3: Insert data into fact and dimension tables

# How to run the Project
### Step 1:
- Access to the AWS to create and Create a new IAM user in your AWS account
- Give it `AdministratorAccess`, From `Attach existing policies directly` Tab
- Take note of the access key and secret
- Edit the file `dwh.cfg` in the same folder as this notebook and fill
### Step 2:
- Run the datawarehouse.ipynb create IAM Role,RedShift and open an incoming  TCP port to access the cluster endpoint
- Get the DWH_ENDPOINT and DWH_ROLE_ARN to complete the file `dwh.cfg` in [CLUSTER] -> HOST and  ARN
### Step 3:
- Run file **create_tables.py** to create new tables
- Run file **etl.py** to execute ETL process

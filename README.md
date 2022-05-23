#Peroject Data Warehouse
This project is from Udacity Data Engneering nanodegree program

#Introduction
The goal of the project is to retrive data from Sparkify, a music stramming startup company to load their data from their s3 bucket and build an ETL pipeline for a Redshift-hosted database.

#Code and Data Structure

##Data Structure 
The staging_events and staging_songs tables are used to store raw data
songplays, users, songs, artists and time tables are part of the star schema. The songplay_table is the fact table and contains the IDs for the relationship to the dimensional tables, rest of them are 


##Queries
All Queries are written in sql_queries.py 

queries include to drop tables if already exists to ensure that there will be no errors in creating the new tables;
creating tables defining data types and their relationships;
copy raw data from S3 storage to cluster database on Redshift;
get data from staging tables and insert into star schema tables;

##ETL Pipeline

The code for extract, transform and load process is in the etl.py file.If you want to run the code, please run  create_tables.py first,and then, run the code using the python3 etl.py command in a terminal. The ETL script connects to Redshift and inserts data from S3 storage into the cluster database. The process is performed through the functions:
load_staging_tables(cur, conn): Iterate over a list of staging table load queries to execute and commit to. Receive parameters to connect to database and connection to data.
insert_tables(cur, conn): Iterate over a list of insert table queries to execute and commit to. Receive parameters to connect to database and connection to data.

#configuration 

Please enter your cluster information and IAM role in the dwh.cofg file


#How to run
Please configure your AWS redshift first, set to public or follow your own role.

run create_tables.py first and then run etl.py

It takes 40 mins to finish the whole process

you can go to your AWS redshift, connect the DB, and run select * from tables you have created and see the results.
Then you can start your analysis.

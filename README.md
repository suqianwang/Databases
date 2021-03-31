# Databases

This repository contains three database projects that use SQL, XML, PostgreSQL, MongoDB, Apache Spark, and AWS

## SQL and PostgreSQL
In this project, I used PostgreSQL database for my data processing tasks:

*Pre-process Raw Data: I used python library psycopg2 to connect to my postgres database in which I created different tables based on the raw data schema for future storage. Then I used python library xml.sax to parse the xml file, clean the raw data, and store them in my tables.
*Data Analysis: I performed several SQL queries to analyze the data. Queries including getting the number of tuples of each table, changing the schema by adding a column and populating it, and more complicated queries on multiple tables to gather information.
*Data Visualization: I performed more queries and visualized the result using table, line graph, and barchart.

## MongoDB
In this project, I used MongoDB to store my data. I wrote several queries analyzing the data. 

## Spark and AWS
In this project, I learned to use Apache Spark and AWS. I two wrote Spark applications in Scala. One is to determine the web pages with no inlinks or outlinks. The other contains the implementation of the PageRank Algorithm and lists out the top 10 web pages. Both applications were tested on local and the AWS cluster. A report was included in the corresponding folder analyzing how the Spark cluster distributes workloads among worker nodes when executing the applications.

# Stock Forecaster Data Storage

This repository provides a Dockerized setup for deploying **Hadoop** and **Hive** to handle large-scale data storage and processing. It is designed to facilitate efficient data analysis for applications such as stock forecasting by leveraging **HDFS** (Hadoop Distributed File System) and **Hive's data warehousing capabilities**. Additionally, it includes scripts and configurations for converting raw data into **Parquet format**, optimizing both storage and query performance.

Web App Repository => [Visit](https://github.com/SudhanshuGulhane/Stock_Forecaster_WebApp)

## Features

- **Dockerized Hadoop and Hive Setup**:
  - Pre-configured Docker environment for seamless deployment of Hadoop and Hive.
  - Supports distributed storage using HDFS and SQL-like querying using Hive.

- **Data Processing with Hive**:
  - Scripts to manage raw data ingestion into Hive tables.
  - Conversion of data into **Parquet format** for better query performance and storage efficiency.

- **Query Optimization**:
  - Demonstrates optimized query techniques using **Common Table Expressions (CTEs)** and **predicate pushdown** for faster results.

## Prerequisites

- **Docker**: Ensure Docker is installed and running on your system.
- **Docker Compose**: Used to orchestrate the services.
- Download companies stock data prices files using Google Sheets function: =GOOGLEFINANCE(company_name, price, DATE(from), DATE(to), frequency).
- Download Complaints.csv file from CFPB Complaint Database: Includes consumer complaints related to various financial issues: credit reporting, debt collection, mortgages, etc 4.29 GB, ~11970000 rows.

## Installation

### Step 1: Clone the Repository

```bash
git clone https://github.com/SudhanshuGulhane/Stock_Forecaster_Data_Storage.git
cd Stock_Forecaster_Data_Storage/docker-hive
```
### Step 2: Deploy the Services
Start the Hadoop and Hive environment using Docker Compose:

```bash
docker-compose up -d
```
This command will:
- Start Hadoop’s NameNode and DataNode services.
- Initialize Hive’s Metastore and Server.

### Step 3: Verify the Setup
Verify the Setup
```bash
docker ps
```

Access Hive CLI within the container:
```bash
docker exec -it hive-server bash
hive
```

## Data Processing Workflow

### Data Ingestion
Follow these steps to prepare and load data into Hive:

- Initialize the Hive Warehouse in HDFS:
  - ```bash
    docker exec -it <namenode_container_id> /bin/bash
    hdfs dfs -mkdir -p /user/hive/warehouse
    hdfs dfs -chmod -R 777 /user/hive/warehouse
    ```
- Transfer the .csv/.ssv/.parquet File to the Docker Container:
  - ```bash
    docker cp /path/to/yourfile.csv <hive-server_container_id>:/tmp/yourfile.csv
    ```
- Move Data to the Hive Warehouse:
  - ```bash
    docker exec -it <hive-server_container_id> /bin/bash
    hdfs dfs -mkdir -p /user/hive/warehouse/your_table
    hdfs dfs -put /tmp/yourfile.csv /user/hive/warehouse/your_table/
    ```
- Create Table in Hive: Use the following query to create a table and ensure it reads directly from the initialized folder:
  - ```bash
    hive
    ```
  - ```sql
    CREATE EXTERNAL TABLE your_table (
        column1 STRING,
        column2 INT
    )
    ROW FORMAT DELIMITED
    FIELDS TERMINATED BY ','
    LOCATION '/user/hive/warehouse/your_table/';

## Results and Performance Evaluation

### Query Performance:
- Parquet format significantly reduces query execution time compared to row storage.
- Average speedup: ~3x faster queries with Parquet.

![Combined metrics comparison-runtime and storage size](https://github.com/user-attachments/assets/4ef0b30e-c728-407d-91d9-8f38a5be37d1)

### Storage Efficiency:
- Parquet compresses data efficiently, reducing storage size by ~3.3x compared to raw CSV/SSV files.

### References:
- Hadoop Official Documentation:(https://hadoop.apache.org/)
- Hive Official Documentation:(https://hive.apache.org/)
- Docker Hive Repository - Big Data Europe Hive: (https://github.com/big-data-europe/docker-hive)

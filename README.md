# YouTube Data Project

## Project Overview

This project is a complete data pipeline for collecting, processing, and analyzing data from the **YouTube Data API**. It follows a typical **ELT** (Extract, Load, Transform) pattern. The pipeline:

- Extracts raw data from YouTube API.
- Loads raw data into a **data lake** (HDFS).
- Transforms raw data into clean format.
- Loads transformed data into a **data warehouse** (Hive).
- Prepares data marts for downstream analysis and reporting.

---

## Architecture

```text
YouTube API --> Extract --> HDFS (data lake) --> Transform --> Load --> Hive (data warehouse)
```

---

## Technologies
- Apache Airflow – Workflow orchestration
- YouTube Data API v3 – Data source
- HDFS – Data Lake 
- Apache Spark – Data transformation
- Apache Hive – Data warehouse
- Docker – Containerized deployment

---

## Project Initialization
1. Clone this repository
<pre> ```git clone https://github.com/phyc11/youtube_data_DE_project.git```  </pre>
<pre> ```cd youtube_data_DE_project``` </pre>
2. Use Docker Compose to build and run
<pre> ```docker-compose build```</pre>
<pre> ```docker-compose up -d```</pre>

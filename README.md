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
YouTube API --> Extract --> HDFS (data lake) --> Transform --> Load --> Hive (data warehouse) --> Superset
```

---

## Technologies

- Apache Airflow – Workflow orchestration
- YouTube Data API v3 – Data source
- HDFS – Data Lake
- Apache Spark – Data transformation
- Apache Hive – Data warehouse
- Apache Super – Data analysis and create dashboard
- Docker – Containerized deployment

---

## Project Initialization

1. Clone this repository

```
git clone https://github.com/phyc11/youtube_data_DE_project.git
```

```
cd youtube_data_DE_project
```

2. Use Docker Compose to build and run

```
docker-compose build
```

```
docker-compose up -d
```

3. Accessing Services
   After the environment is running, access the following services:

| Service          | URL                                            | Username | Password       |
| ---------------- | ---------------------------------------------- | -------- | -------------- |
| Airflow UI       | [http://localhost:8080](http://localhost:8080) | `admin`  | `admin`        |
| Jupyter Notebook | [http://localhost:8888](http://localhost:8888) | token    | auto-generated |
| HDFS Namenode UI | [http://localhost:9870](http://localhost:9870) | N/A      | N/A            |

> To get Jupyter token, run:

```bash
docker logs <jupyter_container_name>
```

### Querying Hive Tables

#### Using Hive via Beeline

1. Enter the Hive server container:

   ```bash
   docker exec -it hive-server /bin/bash
   ```

2. Connect using Beeline:
   ```bash
   beeline -u jdbc:hive2://localhost:10000 -n hive
   ```

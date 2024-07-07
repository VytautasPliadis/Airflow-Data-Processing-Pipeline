# Airflow: Data Processing Pipeline

## Overview
This project aims to analyze the demand for data engineering jobs by aggregating job advertisements from multiple sources. 
It utilizes tools such as Docker, Apache Airflow, and relational databases.

## Features
- Ingest data from multiple job ad sources.
- Orchestrate data ingestion and processing using Apache Airflow.
- Set up and manage a relational database management system (RDBMS).
- Create Dockerfiles and use Docker Compose for the development environment.

## Data Sources
The data for this project will be aggregated from three sources:

- **Remotive**: Public API.
- **We Work Remotely**: RSS feed.
- **Custom CSV file**: Contains job listings.


## Installation

### Prerequisites

- Docker
- Docker Compose
- Python 3.8+

### Steps
**1. Clone the repository:**
```bash
git clone https://github.com/TuringCollegeSubmissions/vypliad-DE3v2.1.5.git cd vypliad-DE3v2.1.5
```

2. **Build and run with Docker:**
```bash
docker-compose up
```

3. **Create two Connections via Airflow UI :**
```
http://localhost:8080
Login: airflow
Password: airflow
Admin -> Connections
```
![fpath.jpg](img%2Ffpath.jpg) 

![ptgres.jpg](img%2Fptgres.jpg)


## Additional metrics in database
The number of new job ads that contain "data engineer" in the title (per day):
```sql
CREATE VIEW new_jobs_per_day AS
SELECT
    DATE(timestamp) AS date,
    COUNT(*) AS new_job_ads
FROM
    jobs
WHERE
    title ILIKE '%data engineer%'
GROUP BY
    DATE(timestamp);

```

The number of new job ads that contain "data engineer" in the title and are remote friendly (per day):
```sql
CREATE VIEW remote_jobs_per_day AS
SELECT
    DATE(timestamp) AS date,
    COUNT(*) AS remote_job_ads
FROM
    jobs
WHERE
    title ILIKE '%data engineer%'
    AND (
        region ILIKE '%remote%'
        OR region ILIKE '%worldwide%'
    )
GROUP BY
    DATE(timestamp);
```

The maximum, minimum, average, and standard deviation of salaries of job ads that contain "data engineer" in the title (total and per day):
```sql
-- Total Salary Statistics
CREATE VIEW total_salary_statistics AS
SELECT
    MAX(CAST(salary AS numeric)) AS max_salary,
    MIN(CAST(salary AS numeric)) AS min_salary,
    AVG(CAST(salary AS numeric)) AS avg_salary,
    STDDEV(CAST(salary AS numeric)) AS stddev_salary
FROM
    jobs
WHERE
    title ILIKE '%data engineer%'
    AND salary ~ '^[0-9]+$';  -- Only numeric values (non-range) are considered
    
-- Daly Salary Statistics
CREATE VIEW daly_salary_statistics AS
SELECT
    DATE(timestamp) AS date,
    MAX(CAST(salary AS numeric)) AS max_salary,
    MIN(CAST(salary AS numeric)) AS min_salary,
    AVG(CAST(salary AS numeric)) AS avg_salary,
    STDDEV(CAST(salary AS numeric)) AS stddev_salary
FROM
    jobs
WHERE
    title ILIKE '%data engineer%'
    AND salary ~ '^[0-9]+$'  -- Only numeric values (non-range) are considered
GROUP BY
    DATE(timestamp)
```

## DAG
![dag.JPG](img%2Fdag.JPG)

## Database Sample
![data.JPG](img%2Fdata.JPG)
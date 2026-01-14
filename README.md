# Netflix Spark ETL Pipeline

## Overview
This project implements an end-to-end ETL pipeline using Apache Spark (PySpark)
to process the Netflix titles dataset and load cleaned, transformed data into
PostgreSQL for analytics.

The pipeline demonstrates:
- Schema-safe CSV ingestion
- Data cleaning and validation
- Business-driven transformations
- Aggregation metrics generation
- JDBC-based loading into PostgreSQL

---

## Tech Stack
- Apache Spark 4.0.1 (PySpark)
- PostgreSQL
- Python 3.10
- JDBC (PostgreSQL Driver)
- Windows 11 (local execution)

---

## Dataset
Source: netflix_titles.csv

The dataset contains information about Netflix movies and TV shows, including:
- show_id
- type (Movie / TV Show)
- title
- director
- cast
- country
- date_added
- release_year
- rating
- duration
- listed_in (genres)
- description

---

## Project Structure

etl/
├── data/
│   └── netflix_titles.csv
├── jars/
│   └── postgresql-42.7.3.jar
├── etl.py
├── spark-warehouse/
└── README.md

---

## ETL Pipeline

### 1. Extract
- Reads the CSV file using an explicit schema
- Avoids Spark inferSchema to prevent type misinterpretation
- Renames reserved SQL keyword `cast` to `cast_members`

### 2. Clean
- Trims whitespace from all string columns
- Converts empty strings to NULL
- Safely parses `date_added` using to_date
- Removes duplicate records based on show_id

### 3. Filter
- Filters titles released from 2015 onwards

### 4. Transform
Business logic applied:
- Normalizes content type and rating
- Extracts season count for TV shows
- Extracts duration in minutes for movies
- Flags long movies (>= 120 minutes)
- Adds data quality flag for missing core fields

### 5. Load
Data is written to PostgreSQL using JDBC.

Tables created:
- netflix_movies (cleaned dataset)
- netflix_genre_metrics (aggregated genre counts)

---

## Database Tables

### netflix_movies
Columns:
- show_id
- type
- title
- director
- cast_members
- country
- date_added
- release_year
- rating
- duration
- listed_in
- description

### netflix_genre_metrics
Columns:
- genre
- titles_count

---

## How to Run

### Prerequisites
- Java 17 or later
- Apache Spark installed and configured
- PostgreSQL running locally
- PostgreSQL JDBC driver available

### Run Command
spark-submit --jars jars/postgresql-42.7.3.jar etl.py

---

## Verification Queries

select count(*) from netflix_movies;

select * from netflix_movies limit 5;

select * from netflix_genre_metrics
order by titles_count desc
limit 10;

---

## Key Engineering Decisions
- Explicit schema to prevent runtime failures
- Safe date parsing to handle malformed data
- No UDFs for better Spark performance
- JDBC batch writes for PostgreSQL reliability
- Clear separation of raw data and metrics

---

## Common Issues Addressed
- Spark timestamp parsing errors
- SQL reserved keyword conflicts
- Windows console encoding issues
- CSV schema drift
- JDBC driver loading on Windows

---

## Future Enhancements
- Incremental loading strategy
- Externalized configuration and secrets
- Structured logging
- Docker-based deployment
- Automated tests

---

## Author
Vaishak

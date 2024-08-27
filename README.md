Overview
========

Welcome to Astronomer! This project was generated after you ran 'astro dev init' using the Astronomer CLI. This readme describes the contents of the project, as well as how to run Apache Airflow on your local machine.

Project Contents
================

Your Astro project contains the following files and folders:

- dags: This folder contains the Python files for your Airflow DAGs. By default, this directory includes one example DAG:
    - `example_astronauts`: This DAG shows a simple ETL pipeline example that queries the list of astronauts currently in space from the Open Notify API and prints a statement for each astronaut. The DAG uses the TaskFlow API to define tasks in Python, and dynamic task mapping to dynamically print a statement for each astronaut. For more on how this DAG works, see our [Getting started tutorial](https://www.astronomer.io/docs/learn/get-started-with-airflow).
- Dockerfile: This file contains a versioned Astro Runtime Docker image that provides a differentiated Airflow experience. If you want to execute other commands or overrides at runtime, specify them here.
- include: This folder contains any additional files that you want to include as part of your project. It is empty by default.
- packages.txt: Install OS-level packages needed for your project by adding them to this file. It is empty by default.
- requirements.txt: Install Python packages needed for your project by adding them to this file. It is empty by default.
- plugins: Add custom or community plugins for your project to this file. It is empty by default.
- airflow_settings.yaml: Use this local-only file to specify Airflow Connections, Variables, and Pools instead of entering them in the Airflow UI as you develop DAGs in this project.

Deploy Your Project Locally
===========================

1. Start Airflow on your local machine by running 'astro dev start'.

This command will spin up 4 Docker containers on your machine, each for a different Airflow component:

- Postgres: Airflow's Metadata Database
- Webserver: The Airflow component responsible for rendering the Airflow UI
- Scheduler: The Airflow component responsible for monitoring and triggering tasks
- Triggerer: The Airflow component responsible for triggering deferred tasks

2. Verify that all 4 Docker containers were created by running 'docker ps'.

Note: Running 'astro dev start' will start your project with the Airflow Webserver exposed at port 8080 and Postgres exposed at port 5432. If you already have either of those ports allocated, you can either [stop your existing Docker containers or change the port](https://www.astronomer.io/docs/astro/cli/troubleshoot-locally#ports-are-not-available-for-my-local-airflow-webserver).

3. Access the Airflow UI for your local Airflow project. To do so, go to http://localhost:8080/ and log in with 'admin' for both your Username and Password.

You should also be able to access your Postgres Database at 'localhost:5432/postgres'.

Deploy Your Project to Astronomer
=================================

If you have an Astronomer account, pushing code to a Deployment on Astronomer is simple. For deploying instructions, refer to Astronomer documentation: https://www.astronomer.io/docs/astro/deploy-code/

Contact
=======

The Astronomer CLI is maintained with love by the Astronomer team. To report a bug or suggest a change, reach out to our support.

# Fire Mitigation Data Engineering Project

## 📖 Introduction

This project aims to develop a comprehensive data engineering solution for fire mitigation using NASA's Fire Information for Resource Management System (FIRMS) data. By leveraging this real-time satellite data, we seek to create an analytical pipeline that can assist in early fire detection, risk assessment, and resource allocation for fire management teams.

## 🛠️ Project Overview

### Problem Statement

Wildfires pose significant threats to ecosystems, property, and human life. Timely detection and efficient resource allocation are crucial for effective fire mitigation. This project addresses the challenge of processing and analyzing large volumes of satellite data to provide actionable insights for fire management teams.

### Data Sources

1. NASA FIRMS (Fire Information for Resource Management System) API
   - Format: JSON
   - Update Frequency: Near real-time (3-hour intervals)
   - Data Points: Active fire locations, fire radiative power, confidence levels

2. Historical Weather Data API (e.g., NOAA API)
   - Format: CSV
   - Update Frequency: Daily
   - Data Points: Temperature, humidity, wind speed, precipitation

### Use Case

The processed data will be used to:
1. Power a real-time fire detection dashboard
2. Generate daily risk assessment reports for fire-prone areas
3. Create a historical database for long-term trend analysis and predictive modeling

### Tech Stack

- Data Ingestion: Apache Kafka
- Data Processing: Apache Spark
- Data Storage: Amazon S3 (data lake) and Amazon Redshift (data warehouse)
- Orchestration: Apache Airflow
- Data Quality: Great Expectations
- Visualization: Tableau

Justification:
- Kafka for handling real-time streaming data from NASA FIRMS
- Spark for distributed processing of large-scale data
- S3 for cost-effective storage of raw and processed data
- Redshift for analytical queries and serving data to dashboards
- Airflow for workflow management and scheduling
- Great Expectations for ensuring data quality throughout the pipeline
- Tableau for creating interactive dashboards for end-users

## 📊 Data Model

[Insert your dimensional model diagram here]

### Data Dictionary

[Insert your data dictionary here, including field names, descriptions, constraints, and transformations]

## 🛠️ ETL Pipeline

1. Data Ingestion
   - Ingest real-time FIRMS data using Kafka
   - Pull daily weather data using batch processing

2. Data Processing
   - Clean and standardize data formats
   - Enrich fire data with weather information
   - Aggregate data for different geographical levels (e.g., county, state)

3. Data Quality Checks
   - Validate data completeness and accuracy
   - Check for anomalies in fire detection data
   - Ensure referential integrity between fact and dimension tables

4. Data Loading
   - Load processed data into Redshift for analytics
   - Update S3 data lake with latest processed data

## 📈 Data Quality Checks

1. NASA FIRMS Data:
   - Check for missing values in crucial fields (latitude, longitude, confidence)
   - Validate that fire detection timestamps are within expected ranges

2. Weather Data:
   - Ensure all required weather parameters are present
   - Check for outliers in temperature and precipitation data

## 🚀 Scaling Considerations

1. Data Volume Increase:
   - Implement data partitioning in S3 for efficient querying
   - Scale up Spark cluster for faster processing
   - Consider using Redshift Spectrum for querying data directly from S3

2. Streaming Requirements:
   - Utilize Spark Streaming for real-time processing
   - Implement a Lambda architecture for combining batch and stream processing

3. Daily Reporting:
   - Set up Airflow DAGs to generate and email reports by 9 AM daily
   - Implement caching mechanisms in Tableau for faster dashboard loading

4. Data Sharing:
   - Implement row-level security in Redshift for controlled access
   - Set up a data catalog using AWS Glue for easy discovery of datasets

## 🌟 Future Enhancements

1. Integrate machine learning models for fire spread prediction
2. Incorporate additional data sources like vegetation indices and topography
3. Develop a mobile app for field teams to access real-time fire information
4. Implement automated alerting system for high-risk fire conditions

## 📊 Project Outcomes

[Include screenshots or links to your dashboard, sample reports, and any other relevant outputs]

---

This README provides an overview of the Fire Mitigation Data Engineering Project. For detailed implementation instructions, please refer to the individual component directories within this repository.

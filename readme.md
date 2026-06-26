# MaterialesProyectoFinal 1.0.0
Final project for the Data Analyst certification Google Cloud Platform (GCP) 

# Overview
Materiales ProyectoFinal is the final project for the Google Cloud Platform (GCP) Data Analyst certification

The project consist of an end-to-end ETL pipeline designed to analyze sales data from a small food busines. The busines owner, Rutilio, sells diffferent food products (mainly tamales) and requires data-driven insights to understand his sales performance-

In this project, data is extracted, transformed using the Medallion Architecture (Bronze, Silver, Gold layers). and loaded into Google BigQuery for analysis and reporting. 

# Problem Statement
Rutilio, a small food busines owner, sells different fod products such as tamles and other prepared meals. However, he currently lacks a structured system to analyze his sales data.

As a result, he is unable to clearly identify:

- Which products generate the highest revenue
- How sales behave over time
- Which products are underperforming
- Overall business performance trends

This projects aims to solve this problem by building and ETL pipeline that transforms raw sales data into structured insights. The final dataset is loaded into Google BigQuery to enable analysis and reporting for better bussines decision-making 

# Features
- Configurable ETL pipeline for flexible data processing 
- Workflow orchestration for managing data pipeline execution
- Medallion architecture implementation (brz, slv, gld) 
- Data quality validation and cleaning processes
- Structured logging system for monitoring pipelne execution
- BigQuery-ready datasets  

# Technologies
- Python
- Pandas
- SQLAlchemy

# Architecture
### Modules
- __Source Data__ 
- __Raw Layer__ 
- __Bronze Layer__ 
- __Silver Layer__ 
- __Gold Layer__ 
- __BigQuery__ 
- __Business Report__

# Installation
### Basic Installation
``` bash
git clone https://github.com/ElApando/MaterialesProyectoFinal.git

cd MaterialesProyectoFinal
```
### Create Virtual Environment
``` bash
python -m venv venv
```
### Activate Environment
``` bash
# Windows
venv/Scripts/Activate
# Linux / Mac
source venv/bin/activate
```
### Install Dependencies
``` bash
pip install -r requirements.txt
```

Example function  

```bash
python -m main
```

# Benefits
- Centralized data pipeline 
- Automated data processing
- Reduced manual intervention
- Improved data quality
- Structured analytical datasets
- Faster business reporting

# Roadmap
### Planned Features
- Support for additional data file formats
- Centralized configuration dictionary for project paths
- Apache Airflow orchestration support
- Docker deployment support
- CI/CD integration with GitHub Actions

# Author
Juan Rodrigo Villalpando González  
Software Engineer | Python Developer | Automation Solutions | Data Engineer | Data Analyst  
LinkedIn: https://www.linkedin.com/in/juan-villalpando-gonzalez  
GitHub: https://github.com/ElApando  


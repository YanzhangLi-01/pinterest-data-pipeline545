# Pinterest Data Engineering Project

## Table of Contents
1. [Project Description](#project-description)
2. [Installation Instructions](#installation-instructions)
3. [Usage Instructions](#usage-instructions)
4. [File Structure](#file-structure)
5. [License Information](#license-information)

## Project Description

This project involves building a scalable, AWS-based data engineering pipeline that simulates Pinterest's data processing capabilities. The pipeline is designed to handle both batch and stream processing of over 30,000 data rows, similar to how Pinterest manages and processes massive amounts of data to enhance user experience. 

### Key Objectives:
- **End-to-End Pipeline**: Implemented a comprehensive data pipeline on AWS, integrating various services to ensure seamless data ingestion, processing, and storage.
- **API Gateway & Kafka**: Set up an API Gateway RESTful API to interface with a Kafka REST proxy, distributing data across three Kafka topics on MSK (Managed Streaming for Apache Kafka).
- **Data Lake**: Utilized MSK Connect to transfer batch data to an AWS S3 data lake, facilitating large-scale data storage and retrieval.
- **Data Processing with Spark**: Created and executed custom Spark transformations on Databricks to clean and aggregate the data in the S3 data lake.
- **Automation**: Automated daily Spark jobs using DAGs (Directed Acyclic Graphs) on Amazon MWAA (Managed Workflows for Apache Airflow).
- **Real-Time Data Streaming**: Implemented real-time data streaming using AWS Kinesis and Spark Structured Streaming for near real-time data analysis.

### What I Learned:
- Leveraging AWS services to build scalable and efficient data pipelines.
- Integrating and automating data workflows using Apache Kafka, Apache Spark, and Databricks.
- Real-time data processing and stream analytics using AWS Kinesis and Spark Structured Streaming.

## Installation Instructions

1. **Clone the Repository**:
   ```bash
   git clone https://github.com/YanzhangLi-01/pinterest-data-pipeline545.git

## Usage Instructions

## File Structure
The project is organized as follows:
```python
pinterest-data-pipeline/       
├── db_creds.yaml 
├── Key pair name.pem
├── user_posting_emulation.py
├── user_posting_emulation_streaming.py
├── pinterest_project_databricks.ipynb
├── 12c7b456b441_dag.py
├── .gitignore
└── README.md
```

## License Information
This project is licensed under the MIT License - see the [LICENSE](https://github.com/git/git-scm.com/blob/main/MIT-LICENSE.txt)
 file for details.
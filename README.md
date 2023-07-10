# Apache-Airflow-Data-Pipeline
- Project completed as part of Udacity's Data Engineering with AWS Nanodegree Program
- Project delivered in July 2023
- Repository includes 10 files:
    * A `SDK_create.ipynb` notebook that contains a series of scripts to create an IAM role, the Redshift cluster, and create the tables in the Redshift cluster
    * An `airflow.cfg` configuration file that contains the parameters for the Redshift cluster and the S3 buckets that contain the source data
    * A `SDK_destroy.ipynb` notebook that contains a series of scripts to remove the IAM role and Redshift cluster created for the project
    * Screenshots of the Apache Airflow UI:
        * `Apache_Airflow-Connections.jpg`
    * For the Apache Airflow folder:
        * Within the `dags` folder, the `final_project.py` that contains the Directed Acyclic Graph (DAG) for the project
        * Within the `plugins/final_project_operators` folder, the `stage_redshift.py`, `load_fact.py`, `load_dimension.py`, `data_quality.py` files that contain the custom operators for the project
        * Within the `plugins/helpers` folder, the `final_project_sql_statements.py` file that contains the SQL queries for the project

## Problem Statement
A music streaming company, Sparkify, has decided to introduce more automation and monitoring to their data warehouse ETL pipelines have chosen to Apache Airflow to do so.

The client expects production-grade data pipelines that are dynamic and built from reusable tasks, can be monitored, and allow easy backfills. They have also noted that the data quality is critical and have requested tests run against their datasets after the ETL steps have been executed to catch any discrepancies.

The source data resides in S3 and needs to be processed in Sparkify's data warehouse in Amazon Redshift. The source datasets consist of JSON logs that tell about user activity in the application and JSON metadata about the songs the users listen to.

## Usage

### Configuration

Set up a config file `airflow.cfg` that uses the following schema. To use the SDK notebooks, you'll have to create access keys for AWS. If these keys are unavailable, you can manually create these assets via the AWS Management Console. Once these assets are available, complete the information for your Redshift cluster and IAM-Role that can manage your cluster and read S3 buckets. I have shared example variables or provided descriptions.

```
[AWS]
KEY = "The AWS Access Key ID is a unique identifier that is used to identify the AWS account associated with an API request. It is a 20-character alphanumeric string, similar to "AKIAIOSFODNN7EXAMPLE". The Access Key ID is used to identify the user or application making API calls to AWS services."
SECRET = "The Secret Access Key is a 40-character alphanumeric string that is paired with the Access Key ID. It is used to sign requests made to AWS services for authentication purposes. The Secret Access Key should be kept confidential and should not be shared or exposed publicly."

#### Example airflow.cfg with recommended inputs included
[DWH]
DWH_CLUSTER_TYPE = multi-node
DWH_NUM_NODES = 4
DWH_NODE_TYPE = dc2.large
DWH_CLUSTER_IDENTIFIER = projectCluster
DWH_DB = #user-input
DWH_DB_USER = #user-input
DWH_DB_PASSWORD = #user-input
DWH_PORT = 5439
DWH_IAM_ROLE_NAME = #user-input

[S3]
LOG_DATA= s3://udacity-dend/log_data
LOG_JSONPATH= s3://udacity-dend/log_json_path.json
SONG_DATA= s3://udacity-dend/song_data
```

*Note: complete the SDK_create notebook to generate the following inputs for the Airflow Connections tab*
* ARN = "An Amazon Resource Name (ARN) is a unique identifier for resources within Amazon Web Services (AWS). It is a string of characters that follows a specific format and is used to identify and access various AWS resources."
* HOST = "The endpoint of a Redshift cluster typically follows the format: <cluster-identifier>.<random-characters>.<region>.redshift.amazonaws.com"
* DB_NAME = #user-input
* DB_USER = #user-input
* DB_PASSWORD =  "When setting up an AWS Redshift cluster, it is recommended to follow best practices for password management, such as using strong and unique passwords, regularly rotating passwords, and ensuring proper access control."
* DB_PORT = 5439

### Apache Airflow Instructions

Once your Redshift cluster is created, ensure you have your Apache Airflow and Conda environment set-up.

You'll need two connections in the Airflow UI: one for your Redshift cluster and one for your AWS credentials. The Redshift connection should be of type `redshift` and the AWS connection should be of type `aws`. I have included a screenshot of the connections I created in the Airflow UI, Apache_Airflow-Connections.jpg.

The Direct Acyclic Graph (DAG) is set-up to run once an hour. You can change the schedule interval in the DAG script. Once the DAG is running, you can monitor the progress in the Airflow UI. You can also monitor the progress of the individual tasks in the Airflow UI. I have included a screenshot of the Graph view of the DAG in the Airflow UI, Apache_Airflow-DAG.jpg.

## Solution

### Techincal Discussion
My proposed DAG (Directed Acyclic Graph) solution for the ETL process is comprised of several key tasks:

1.  Creating Tasks: The first seven tasks in the pipeline are to create the Redshift tables. I created a custom CreateTableOperator to run SQL queries listed at the top of the script.

2.  Staging Tasks: The second pair of tasks in the pipeline load the JSON data from S3 into staging tables in Redshift. I am using COPY commands to move data in parallel, taking advantage of Redshift's MPP (Massively Parallel Processing) capabilities to speed up the transfer.

3.  Transformation and Load Tasks: After staging, the pipeline will run SQL transformations that convert and load the staging data into a star schema, with a fact table *songplays* and four dimensional tables *artists*, *songs*, *users*, and *time*.

4. Data Quality Checks: The final task in the pipeline are data quality checks. Here, I ran custom SQL queries on the Redshift tables to ensure data quality, such as checking for null primary keys, or ensuring that certain tables have data.

This pipeline is dynamic and reusable because it's defined in Python code, uses Airflow's dynamic DAG generation capabilities, and makes use of Airflow's powerful operators. These operators provide template capabilities for SQL queries and S3 and Redshift connections, allowing us to minimize code repetition.

### Business Justification
Implementing this solution will streamline our ETL processes and ensure the high quality of our data. Apache Airflow will allow us to monitor our pipelines closely, visually track progress, and quickly identify any issues that arise, thus minimizing downtime and maintaining a consistent service for our users.

Furthermore, the data quality checks we're implementing will ensure that the data in our warehouse is reliable and accurate, supporting sound business decisions. This is a critical measure to prevent data discrepancies that could lead to flawed analyses and forecasts.

Lastly, the dynamism and reusability of the DAG solution make it an efficient and cost-effective choice. This approach will be flexible to accommodate any changes to our data or business needs, and will reduce the time and resources needed to maintain or modify our pipelines. This, combined with improved data quality, will enhance our ability to leverage our data for strategic insights and maintain a competitive edge in the market.

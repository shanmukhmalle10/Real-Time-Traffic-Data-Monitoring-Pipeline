#   Real-Time Traffic Data Monitoring Pipeline (Databricks + PySpark)

## Project Overview 

The Real-Time Traffic Data Monitoring Pipeline is a scalable data engineering solution designed to ingest, process, and analyze streaming traffic data for real-time insights. This project leverages modern Lakehouse architecture using Databricks, PySpark, Delta Lake, and AWS services to build an end-to-end pipeline that handles high-volume traffic sensor data efficiently. Traffic data (vehicle count, speed, timestamps, etc.) is streamed through Amazon Kinesis, processed in near real-time, and stored across Bronze, Silver, and Gold layers in Delta Lake for structured analytics and reporting.

The pipeline ensures:
- Reliable ingestion of streaming data
- Data cleansing and validation
- Real-time computation of traffic metrics
- Scalable storage and optimized querying
---
## Project Objective
- Build a scalable real-time traffic monitoring pipeline to handle continuous data from sensors  
- Process high-volume streaming data using AWS Kinesis for near real-time ingestion  
- Store and transform data in Databricks Delta Lake using Medallion Architecture
- Ensure high data quality by filtering invalid and corrupt records
- Generate analytics for congestion detection, traffic patterns, and anomaly identification  
- Provide interactive dashboards to support real-time monitoring and decision-making
- Enable testing and validation using Pytest for data accuracy
- Set up alerting mechanisms (e.g., Slack notifications) for abnormal traffic conditions



---

##   Dataset

### Dataset Source  
Simulated Traffic Sensor Data (Streaming via Amazon Kinesis)

The dataset represents real-time traffic data collected from sensors across city roads.

https://www.kaggle.com/datasets/sonalsadia/traffic-detection-dataset

---

### Datasets Used

- Traffic Readings → Vehicle count, speed, congestion  
- Vehicle Metrics → Flow rate, density, delay  
- Speed Monitoring → Speed violations and variance  
- Incident Events → Accidents and disruptions  
- Signal Performance → Traffic signal efficiency  

These datasets simulate a **real-world smart city traffic system**.

---
##   Technologies Used

| Technology      | Purpose                         |
| --------------- | ------------------------------- |
| Python          | Data processing & scripting     |
| PySpark         | Distributed data processing     |
| Databricks      | Data engineering platform       |
| AWS S3          | Data lake storage               |
| AWS Lambda      | Serverless data ingestion       |
| Amazon Kinesis  | Real-time data streaming        |
| Amazon Firehose | Stream delivery to S3 (Parquet) |
| AWS Glue        | Metadata management             |
| Apache Airflow  | Workflow orchestration          |
| Slack           | Alerts & notifications          |

---
## LakeHouse Architecture

<img width="1536" height="1024" alt="ChatGPT Image Mar 18, 2026, 04_27_27 PM (1)" src="https://github.com/user-attachments/assets/ca3d00ce-1635-4521-821a-9efd6ab368aa" />

---
# ETL Pipeline Design 

## Bronze Layer (Raw Data)

### Purpose

- Store raw data exactly as received
- Preserve data lineage
- Enable traceability of raw ingestion

### Tables

```
traffic_catalog.bronze.incident
traffic_catalog.bronze.sensor
traffic_catalog.bronze.signal
traffic_catalog.bronze.speed
traffic_catalog.bronze.vehicle
```

### Operations

- Raw CSV ingestion from AWS S3
- Schema validation
- Metadata registration via AWS Glue

---

## Silver Layer (Cleaned Data)

### Purpose

- Clean and standardize datasets
- Integrate multiple datasets

### Transformations

- Remove duplicate records
- Convert data types
- Handle missing values
- Join sales data with store metadata
- Join transaction data with sales records
- Extract date features

### Output Table

```
traffic_catalog.silver.incident_clean
traffic_catalog.silver.sensor_clean
traffic_catalog.silver.signal_clean
traffic_catalog.silver.speed_clean
traffic_catalog.silver.vehicle_clean
```

---

## Gold Layer (Analytics Data)

### Star Schema

<img width="1536" height="1024" alt="ChatGPT Image Mar 19, 2026, 02_20_56 PM" src="https://github.com/user-attachments/assets/3b89581c-9b04-4c4b-b783-0072e4892d08" />



### Purpose

Generate **business-ready datasets for analytics and forecasting**.

### Features Generated

- Average Speed (avg_speed): Calculates mean vehicle speed for each sensor and time window

- Vehicle Count (vehicle_count): Total number of vehicles detected per event or interval

- Congestion Flag (congestion_flag): Binary indicator (0/1) showing whether traffic is congested based on speed thresholds

- Congestion Level (congestion_level): Categorizes traffic into Low / Medium / High based on speed and volume

- Rolling Average Speed (10min_avg_speed): Computes moving average speed over a 10-minute window for trend analysis

- Traffic Density: Derived from vehicle count and road capacity to measure load on roads

- Signal Efficiency Metrics": Measures performance of traffic signals (e.g., wait time, flow efficiency)

- Time-Based Aggregations: Metrics analyzed by hour, day, and date for trend analysis
- Anomaly Detection Flags: Identifies unusual traffic patterns or sudden drops/spikes

---

# **Air Flow** (Pipeline Orchestration)

The pipeline is orchestrated using **Apache Airflow DAGs**.
![WhatsApp Image 2026-03-19 at 14 14 06](https://github.com/user-attachments/assets/6b0780f9-4ded-4a8c-b423-ad74efdd3b5e)







### Airflow DAG Tasks

```
Task 1: Bronze Pipeline
Task 2: Silver Pipeline
Task 3: Gold Pipeline
```

### Scheduling


Pipelines run on a **daily schedule** for automated data processing.

---
### Alerts
<img width="1417" height="621" alt="Screenshot 2026-03-19 at 2 30 30 PM" src="https://github.com/user-attachments/assets/29a37f23-577b-408b-9fdc-3d71770f0fd1" />

* Integrated Slack alerts in **Apache Airflow DAG** to automatically notify on task failures with details like DAG ID, task ID, execution time, and log links.
* Configured success notifications to send pipeline completion status and runtime metrics to Slack for real-time monitoring.
---


##   Data Quality Checks

Implemented checks include:

- Null value validation  
- Duplicate detection  
- Schema validation  
- Invalid data filtering  
- Streaming error handling  

### Monitoring

- Databricks logs  
- Streaming checkpoints  
- Error logging tables


---

# Analytics Dashboards & Artifacts

This section contains **dashboards generated from the analytics (Gold layer) dataset**.

## Signal Performance 
<img width="1171" height="845" alt="Screenshot 2026-03-19 at 2 32 34 PM" src="https://github.com/user-attachments/assets/53a190a1-ab13-4ce2-9088-afcfc613668d" />

## Incident & Speed Analysis
<img width="981" height="865" alt="Screenshot 2026-03-19 at 2 34 16 PM" src="https://github.com/user-attachments/assets/5c062d4b-89a1-4b7c-a3ee-5c3d65e80788" />

## Traffic overview

---<img width="1047" height="856" alt="Screenshot 2026-03-19 at 2 35 23 PM" src="https://github.com/user-attachments/assets/d7d4993c-4c14-4ec3-ac9e-afaca951022a" />

---
##   Business Insights Generated

The pipeline enables several **smart city traffic insights**:
- Network Health

  - Traffic network remains stable with scores between 49–52

  - Indicates consistent but moderately congested conditions

- Peak Hour Trends

  - Highest congestion during morning and evening peak hours

  - Lower congestion during midday and late night

- Congestion Hotspots

  - Certain sensors/locations consistently show high congestion

  - Identifies major traffic bottleneck areas

- Incident Analysis

  - Accidents, breakdowns, and roadblocks increase congestion

  - Leads to higher response and clearance times

- Signal Performance

  - Total Signals: 863

  - Poor Signals: 742

  - Average Efficiency: ~49%

  - Average Wait Time: ~90 seconds

  - Indicates inefficient signal management

- Intersection Delays

  - Some intersections experience wait times above 180 seconds

  - Major contributor to peak-hour congestion

- Vehicle Impact

  - Heavy vehicles (trucks/buses) contribute more to congestion

  - Especially during peak hours

- Weekday vs Weekend

  - Weekdays show slightly higher congestion than weekends

  - Traffic remains consistently high overall

- Lane Blockage Impact

  - Lane closures significantly increase congestion levels

  - Requires quick incident resolution

- Vehicle Distribution

  - Traffic is evenly distributed across vehicle types

  - Requires balanced traffic management strategies




---


# Author

### Project Lead

**Venkata Mahesh Babu Danduboina**

### Team Members

- Sandeep Reddy
- Shanmukh Reddy
- Umesh yadav



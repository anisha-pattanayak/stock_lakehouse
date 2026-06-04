# Real-Time Stock Market Lakehouse Platform

A real-time data engineering project that simulates how stock market data can be collected, processed, stored, and visualized using a modern lakehouse architecture.

The project focuses on building an end-to-end streaming pipeline for stock market analytics using Kafka, PySpark, Delta Lake, FastAPI, and Streamlit.

## Project Overview

This platform ingests stock market data from an external API, streams it through Kafka, processes it using PySpark, stores the data in Delta Lake, and makes the final analytics available through APIs and dashboards.

The main goal of this project is to demonstrate practical knowledge of real-time data pipelines, lakehouse architecture, streaming data processing, API development, dashboarding, and containerized deployment.

## High-Level Architecture

```text
Stock Market API
      ↓
Kafka Producer
      ↓
Apache Kafka
      ↓
PySpark Structured Streaming
      ↓
Delta Lake
(Bronze → Silver → Gold)
      ↓
FastAPI
      ↓
Streamlit Dashboard
```

## Architecture Flow

The project follows a real-time data pipeline approach:

* **Stock Market API:** Source of live or near real-time stock data
* **Kafka Producer:** Collects data from the API and sends it to Kafka
* **Apache Kafka:** Handles real-time data streaming
* **PySpark Streaming:** Processes and transforms incoming data
* **Delta Lake:** Stores data in structured lakehouse layers
* **FastAPI:** Provides API endpoints for accessing processed data
* **Streamlit Dashboard:** Displays stock trends and analytics visually

## Lakehouse Layers

The storage layer follows the Bronze, Silver, and Gold architecture:

* **Bronze Layer:** Stores raw stock market events
* **Silver Layer:** Stores cleaned and validated stock data
* **Gold Layer:** Stores aggregated data for analytics, such as moving averages, OHLCV data, and volatility metrics

## Key Features

* Real-time stock data ingestion using Kafka
* Streaming data processing with PySpark
* Lakehouse-based storage using Delta Lake
* REST API layer using FastAPI
* Interactive dashboard using Streamlit
* Docker-based local development setup
* Basic monitoring support with Prometheus and Grafana

## Tech Stack

* **Programming Language:** Python
* **Streaming:** Apache Kafka
* **Processing:** PySpark Structured Streaming
* **Storage:** Delta Lake
* **API:** FastAPI
* **Dashboard:** Streamlit
* **Database:** PostgreSQL
* **Orchestration:** Apache Airflow
* **Monitoring:** Prometheus, Grafana
* **Containerization:** Docker

## What I Learned

Through this project, I worked on designing and building a complete real-time data engineering workflow. It helped me understand how streaming data pipelines work, how raw data is transformed into analytics-ready data, and how APIs and dashboards can be used to make processed data useful for end users.

This project also improved my understanding of Docker-based environments, data pipeline structure, and the role of monitoring in modern data platforms.

## Future Improvements

* Add more stock market data providers
* Improve anomaly detection logic
* Deploy the pipeline on cloud infrastructure
* Add authentication for API access
* Build more advanced financial analytics dashboards

## License

This project is created for learning and portfolio purposes.

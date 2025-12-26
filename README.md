# Game Analytics System

A comprehensive, microservice-based web application for real-time video game analytics. It simulates, processes, and visualizes game telemetry data using a modern event-driven architecture.

## Architecture

The system is built as a **Monorepo** (Turborepo-style) using **Docker** for orchestration. The data flows through the system as follows:

1.  **Data Generation (`faker` service):** A Python script simulates realistic player data and game events using Markov chains and behavioral personas. This data is published to a Kafka topic.
2.  **Event Streaming (Kafka):** Apache Kafka acts as the central event bus, decoupling data producers from consumers and enabling real-time data processing.
3.  **ETL and Orchestration (Airflow):** An Airflow DAG (`game_analytics_star_schema_etl`) runs periodically to perform the following tasks:
    *   Ingests data from Kafka and stores it in MongoDB (hot storage).
    *   Performs an ETL process to move the data from MongoDB into a PostgreSQL database with a star schema.
    *   Archives old data from MongoDB to Hadoop HDFS for long-term storage.
    *   Runs data quality checks.
4.  **Backend API (`api` service):** A FastAPI application provides a RESTful API to query the analytics data from the PostgreSQL database. It uses Redis for caching to improve performance.
5.  **Frontend Dashboard (`web` service):** A Streamlit application provides a user-friendly interface for visualizing the analytics data. It interacts with the backend API to fetch the data and presents it in interactive charts and graphs.

### Services

*   **`apps/api`**: FastAPI backend service.
*   **`apps/faker`**: AI-driven data simulation service.
*   **`apps/spark-processor`**: Spark-based data processing service.
*   **`apps/web`**: Streamlit frontend for data visualization.
*   **`airflow`**: Airflow DAGs for ETL orchestration.
*   **`packages/game_library`**: Shared Python library for game-related data.

## Tech Stack

*   **Language**: Python 3.11
*   **Web Frameworks**: Streamlit (UI), FastAPI (API)
*   **Data Streaming**: Apache Kafka, Zookeeper
*   **Databases**:
    *   **PostgreSQL**: For storing the analytical data in a star schema.
    *   **MongoDB**: For hot storage of incoming data from Kafka.
    *   **Redis**: For caching API responses.
    *   **Hadoop (HDFS)**: For long-term archival of old data.
*   **Orchestration**: Docker, Docker Compose, Apache Airflow
*   **Monorepo Management**: Turborepo

## Quick Start

### Prerequisites

*   Docker & Docker Compose

### Running the Stack

1.  **Clone the repository and start all services:**
    ```bash
    docker-compose up --build
    ```
    This command will build and start all the services, including the databases, Kafka, Airflow, and the application services.

2.  **Access the applications:**
    *   **Web Dashboard**: [http://localhost:8501](http://localhost:8501)
    *   **API Docs**: [http://localhost:8000/docs](http://localhost:8000/docs)
    *   **Airflow UI**: [http://localhost:8080](http://localhost:8080) (user: `admin`, pass: `admin`)

### Service Endpoints & Credentials

*   **PostgreSQL**:
    *   **Host**: `localhost:5432`
    *   **User**: `rafay`
    *   **Password**: `rafay`
    *   **Database**: `game_analytics`
*   **MongoDB**:
    *   **Host**: `localhost:27017`
    *   **User**: `admin`
    *   **Password**: `admin`
*   **Kafka Broker**: `localhost:9092`
*   **Redis**: `localhost:6379`

## Development

This project is a Turborepo monorepo. You can use the following commands for development:

*   `npm run build`: Build all applications.
*   `npm run dev`: Run all applications in development mode.
*   `npm run lint`: Lint the codebase.

Built by Syed Rafay Ahmed 27071 and Ali Siddiqi 26902
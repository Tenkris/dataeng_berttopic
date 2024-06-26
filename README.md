# Project README

This repository provides instructions to set up Apache Airflow and Kafka using Docker.

## Prerequisites

- Python installed on your machine.
- Docker installed on your machine.

## Setup Apache Airflow

1. Install Apache Airflow using pip:
   ```
   pip install apache-airflow
   ```

2. Initialize Airflow metadata database:
   ```
   airflow db init
   ```

3. (Optional) Review and configure Airflow settings:
   ```
   airflow config list
   ```

4. Create an admin user for Airflow web UI:
   ```
   airflow users create --role Admin --username admin --email admin --firstname admin --lastname admin --password admin
   ```

5. Start the Airflow webserver on port 8080:
   ```
   airflow webserver --port 8080
   ```

6. Copy your `dag.py` file to the `dags` folder in your Airflow installation directory.

## Set Up Kafka

1. Install Docker if not already installed.

2. Navigate to your project directory containing the `docker-compose.yml` file.

3. Start Kafka and related services using Docker Compose:
   ```
   docker-compose up -d
   ```
   This command will start Kafka and other services defined in the `docker-compose.yml` file in detached mode.

## Troubleshooting and Support

If you encounter any issues or have any questions, feel free to reach out!

---

You can now copy this text directly into your README file.

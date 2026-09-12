# ==============================================================================
# Dockerfile for Apache Airflow with Pipeline Dependencies
# ==============================================================================
FROM apache/airflow:2.8.1-python3.10

# Switch to airflow user for pip installation
USER airflow

# Copy requirements and install python packages
COPY requirements.txt /requirements.txt
RUN pip install --no-cache-dir -r /requirements.txt

# Ensure Airflow scheduler and worker can import modules from scripts
ENV PYTHONPATH="${PYTHONPATH}:/opt/airflow:/opt/airflow/scripts"

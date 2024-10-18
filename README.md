# 240-531 Data Engineering
Docker Container Deployment for MSSQL and Apache Airflow
## Author Information
- Name: Wathunyu Phetpaya
- Student Code: 6710120039
- Institution: Prince of Songkla University
- Department: Computer Engineering
- Degree: Master
## Docker Container Deployment
### Configure Environment Variables
Edit the .env file to specify the correct values for
1. MSSQL_SERVER_PRODUCTION
2. MSSQL_SERVER_DEVELOPMENT
### Deploy Containers
3. docker compose up -d
## Docker Container Configuration
### Access the Airflow Scheduler Container
4. docker exec -it airflow-airflow-scheduler-1 bash
### Install Required Python Packages
5. cd /opt/airflow/
6. pip install -r requirements.txt
## Access Apache Airflow Web Interface
7. URL: http://localhost:8080 OR URL: http://127.0.0.1:8080
## Access Jupyter Web Interface
8. URL: http://localhost:8888 OR URL: http://127.0.0.1:8888
## Export Boxplot Images
9. docker cp airflow-airflow-scheduler-1:/tmp/receipt_boxplot.png ./receipt_boxplot.png
10. docker cp airflow-airflow-scheduler-1:/tmp/contract_boxplot.png ./contract_boxplot.png
11. docker cp airflow-airflow-scheduler-1:/tmp/member_boxplot.png ./member_boxplot.png

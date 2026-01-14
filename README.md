# Studies with Airflow
created a docker-compose file with services: **Airflow**, **PostgreSQL**, **MinIO**. <br />
Using Admin -> Connections -> Add Connection (Here I can use to create a connection between Airflow and other services [PostgreSQL, MinIO (AWS Service), Databricks]) <br />
<img width="338" height="412" alt="1_postgres_connection" src="https://github.com/user-attachments/assets/79c1ebe9-0cd8-443b-88e2-c223c40666f5" />

## Folders:
### dags/airflow_connect_db
<img width="110" height="34" alt="Screenshot 2026-01-14 at 12 11 19 AM" src="https://github.com/user-attachments/assets/f134b544-ec97-490c-ab87-aa36e491adad" />
<img width="460" height="160" alt="2_dag_execute_sql" src="https://github.com/user-attachments/assets/c1f3bdda-32d5-4987-9df1-6573446c07e2" />

Dag will create a table in Postgres and insert data, all the sql files will be stored in: sql/ directory.

### dags/tutorial_load_data/
1. test_minio_dag 
<img width="101" height="39" alt="Screenshot 2026-01-14 at 12 16 32 AM" src="https://github.com/user-attachments/assets/41e79216-ee4f-4bd7-8e7c-59efebc05d83" />
<img width="128" height="65" alt="Screenshot 2026-01-14 at 12 16 27 AM" src="https://github.com/user-attachments/assets/1cefcdab-4b5c-48ec-8cc8-dbefd6f33a86" /><br />
Dag will test the connection from Airflow to MinIO, as the button test was failing in the Airflow API. <br />

<img width="314" height="174" alt="Screenshot 2026-01-14 at 12 19 02 AM" src="https://github.com/user-attachments/assets/0d65ac55-599e-4d8d-9f82-a48de4554589" /><br />
2. ny_covid_data<br />
<img width="318" height="33" alt="Screenshot 2026-01-14 at 12 20 58 AM" src="https://github.com/user-attachments/assets/58eafd93-569c-4351-8e66-b886c89a2174" />
<img width="237" height="80" alt="Screenshot 2026-01-14 at 12 21 02 AM" src="https://github.com/user-attachments/assets/a9df2162-f833-4bb8-89b5-2c11fe35401b" /> <br />
Dag will read json file from a url, upload the file in MinIO (but, it will check if the file is already in the bucket) and execute some validation with the data.

### dags/first_sensor/
1. dag_s3<br />
   <img width="178" height="29" alt="Screenshot 2026-01-14 at 12 25 34 AM" src="https://github.com/user-attachments/assets/5a22214e-ea30-4507-8b59-0fb8ec58bab5" />
<img width="249" height="60" alt="Screenshot 2026-01-14 at 12 25 38 AM" src="https://github.com/user-attachments/assets/7bdeebc1-e058-483d-ab9a-d32e3f499f4e" /> <br />
Dag will check if a file arrived inside the bucket.


# Globant-Coding-Challenge
 This is the repository for the Globant Data Engineer Coding Challenge. The API is based on Flask, SQLAlchemy and a PostgreSQL connection to handle the data and respective queries. The app is containerized with Docker to allow for easy deployment regardless of the system.

## Description
- Section 1: API
In the context of a DB migration with 3 different tables (departments, jobs, employees) , create
a local REST API that must:
    1. Receive historical data from CSV files
    2. Upload these files to the new DB
    3. Be able to insert batch transactions (1 up to 1000 rows) with one request

    - Clarifications:
        + You decide the origin where the CSV files are located.
        + You decide the destination database type, but it must be a SQL database.
        + The CSV file is comma separated.

- Section 2: SQL

    1. Number of employees hired for each job and department in 2021 divided by quarter. The table must be ordered alphabetically by department and job.
    2. List of ids, name and number of employees hired of each department that hired more employees than the mean of employees hired in 2021 for all the departments, ordered by the number of employees hired (descending).

## Libraries Used

- Flask - Creation of the local REST API 
- SQLAlchemy - Manage the ORM (Object-Relational Mapping), the database engine instance, and the insert library to correctly UPSERT records
- Pandas - Creation of dataframes given the 3 CSV (Jobs, Departments and hired employees) and reading/writing on PostgreSQL database with to_sql and from_sql functions
- Psycopg - Mainly used for both SQL end-points to retrieve results from PostgreSQL
- OS - Creation and reading of environmental variables for the database URI and respective credentials.
- pytest - Enables the creation of test for the API
- Unit Test Mock - Allows to create mock functionalities to replicate the operation of the API and run tests

## Installation
Clone the repository
```sh
git clone https://github.com/AndresFR28/Globant-Coding-Challenge.git
```

Go to the repositories directory
```sh
cd Globant-Coding-Challenge
```

Build and start the containers
```sh
docker-compose up --build
```

Stop and remove the containers
```sh
docker-compose down
```
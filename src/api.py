from flask import Flask
from flask_sqlalchemy import SQLAlchemy
from sqlalchemy import create_engine
from sqlalchemy.sql import text
from sqlalchemy.dialects.postgresql import insert
import pandas as pd
import psycopg2
from psycopg2 import sql
import os
from dotenv import load_dotenv # type: ignore

load_dotenv()
api = Flask(__name__)

host, pgdatabase, port, pguser, password = os.getenv("POSTGRES_HOST"), os.getenv("POSTGRES_DB"), os.getenv("POSTGRES_PORT"), os.getenv("POSTGRES_USER"), os.getenv("POSTGRES_PASSWORD")
db_URI = f'postgresql://{pguser}:{password}@{host}/{pgdatabase}'
api.config["SQLALCHEMY_DATABASE_URI"] = db_URI
api.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False
db = SQLAlchemy(api)

'''
#Create PostgreSQL Database
def createDatabase():
   connection = psycopg2.connect(
      user = pguser,
      password = password,
      host = host,
      port = port
   )
   cursor = connection.cursor()
   connection.autocommit = True
   query = sql.SQL(f"CREATE DATABASE {pgdatabase}")
   cursor.execute(query)
   connection.commit()
   cursor.close()
   connection.close()

createDatabase()
'''

#Define Table Schemas with Alchemy
#Department Schema
class DepartmentSchema(db.Model):
   __tablename__ = 'department'
   department_id = db.Column(db.Integer, primary_key=True) #INTEGER Id of the department
   department = db.Column(db.String(50), nullable=False) #STRING Name of the department

#Job Schema  
class JobSchema(db.Model):
   __tablename__ = 'job'
   job_id = db.Column(db.Integer, primary_key=True) #INTEGER Id of the job
   job = db.Column(db.String(50), nullable=False) #STRING Name of the job

#Employee Schema
class EmployeeSchema(db.Model):
   __tablename__ = 'employee'
   employee_id = db.Column(db.Integer, primary_key=True) #INTEGER Id of the employee
   name = db.Column(db.String(50), nullable=True) #STRING Name and surname of the employee
   datetime = db.Column(db.String(20), nullable=True) #STRING Hire datetime in ISO format
   department_id = db.Column(db.Integer, db.ForeignKey('department.department_id'), nullable=True) #INTEGER Id of the department which the employee was hired for
   job_id = db.Column(db.Integer, db.ForeignKey('job.job_id'), nullable=True) #INTEGER Id of the job which the employee was hired for

#Create tables on PostgreSQL based on Schemas.
with api.app_context():
   try:    
      db.create_all()
   except Exception:
      print("PostgreSQL Username or password are incorrect - 401")
      #Quit script if connection to PostreSQL is not properly set up
      quit()

#APIs Home
@api.route("/")
def home():
   return "<h1>Globant API<h1>"


#End-point = Receive historical data from CSV files and upload them into postgreSQL DB named gproject
@api.route("/api/v1/upload_historical_data", methods = ["GET", "POST"])
def upload_historical_data():
   
   #Define paths to the historical CSV
   depPath = r"data/Historical/departments.csv"
   jobPath = r"data/Historical/jobs.csv"
   empPath = r"data/Historical/hired_employees.csv"
   
   #Define status that will hold the current state of the end point
   status = ""
   
   #Check if both Job and Department tables where created
   primaryCheck = 0

   #Create the database engine connection
   engine = create_engine(db_URI)

   #Check if the department path and job path with the historical CSVs exist
   if (os.path.exists(depPath) and os.path.exists(jobPath)):
      
      if (os.path.getsize(depPath)) != 0 and (os.path.getsize(jobPath)) != 0:

         #Read path for historical data for Job and Department and create pandas dataframes based on those CSV
         departments = pd.read_csv(depPath, names=["department_id", "department"])
         jobs = pd.read_csv(jobPath, names=["job_id", "job"])

         #Truncate all the tables to add new historical data
         db.session.execute(text("TRUNCATE TABLE job,department,employee"))
         db.session.commit()
         
         #Use pandas dataframe to write data into the PostgreSQL tables
         try:
            departments.to_sql('department', engine, if_exists='append', index=False)
            status = status + "<p>Data uploaded to Department table successfully! - 200</p>"
         except:
            status = status + "<p>ERROR: Review the Department table Schema and the CSV file. Either the number of columns is different or the datatypes vary - 500</p>"
            primaryCheck = 1
         
         try:
            jobs.to_sql('job', engine, if_exists='append', index=False)
            status = status + "<p>Data uploaded to Job table successfully! - 200</p>"
         except:
            status = status + "<p>ERROR: Review the Job table Schema and the CSV file. Either the number of columns is different or the datatypes vary - 500</p>"
            primaryCheck = 1

         #Get valid IDs for job_id and department_id from their respective tables
         job_valid_ids = tuple(jobs['job_id'])
         dep_valid_ids = tuple(departments['department_id'])
         
         if os.path.exists(empPath):
            if os.path.getsize(empPath) != 0:
               if primaryCheck == 0:
                  #Read path for historical data for employee and create pandas dataframe based on CSV
                  employees = pd.read_csv(empPath, names=["employee_id", "name", "datetime", "department_id", "job_id"])

                  #Create a dataframe with invalid IDS, ergo, IDs present in the employee CSV as Foreign Keys that don't actually exist in the job and department CSV as primary keys 
                  emp_invalid = employees[(~employees['job_id'].isin(job_valid_ids) & employees['job_id'].notnull()) | (~employees['department_id'].isin(dep_valid_ids) & employees['department_id'].notnull())]
                     
                  #If there are invalid IDs, don't insert historical employee data
                  if emp_invalid.size != 0:
                     status = status + "<p>ERROR: Department ID or Job ID not valid. Foreign key constraint on Employee table not met. Check data and schema - 500</p>"
                  #If there are no invalid IDs, insert historical employee data
                  else:
                     try:
                        employees.to_sql('employee', engine, if_exists='append', index=False)
                        status = status + "<p>Data uploaded to Employee table successfully! - 200</p>"
                     except:
                        status = status + "<p>ERROR: Review the Employee table Schema and the CSV file. Either the number of columns is different or the datatypes vary - 500</p>"
               else:
                  status = status + "<p>ERROR: Either Job table or Department table couldn't be created. Given the foreign key constraints, no data was uploaded to the Employee table - 500</p>"
            else:
               status = status + "<p>ERROR: Employee CSV is empty. No data was uploaded for the Employee table - 500</p>"
         else:
            status = status + "<p>ERROR: Employee CSV is not present on the path. No data was uploaded for the Employee table - 500</p>"
      else:
         status = status + "<p>ERROR: Job CSV or Department CSV are empty. No data was uploaded - 500</p>"
   else:
      status = status + "<p>ERROR: Job CSV or Department CSV are not present on the path. No data was uploaded - 500</p>"
   
   return status


#End-point = Insert up to 1000 rows in batch transactions into postgreSQL DB named gproject
@api.route("/api/v1/insert_data", methods = ["GET", "POST"])
def insert_data():
   #Define paths to the new CSV that contain data that will be appended to the already created tables
   depPath = r"data/New/departments.csv"
   jobPath = r"data/New/jobs.csv"
   empPath = r"data/New/hired_employees.csv"

   #Set status in case no files exist in the specified path
   iniStatus = "<p>No new files were found to insert batch data</p>"
   status = ""
   statusCheck = 0

   #Create the database engine connection
   engine = create_engine(db_URI)

   #Check if the department path with the new CSV file exist
   if os.path.exists(depPath):
      if os.path.getsize(depPath) != 0:
         statusCheck = 1
         #Retrieve both historical file and the new file for departments
         departmentNew = pd.read_csv(depPath, names=["department_id", "department"])
         departmentHistorical = pd.read_sql('department', engine)

         #Check if both dataframes have the same schema (Columns and datatypes)
         if departmentNew.dtypes.equals(departmentHistorical.dtypes):

            #JOIN both dataframes to see the IDs that match and new IDs
            dep_matched_ids = tuple(pd.merge(departmentHistorical,departmentNew,on='department_id',how='right')['department_id'])
            
            
            #Create new dataframe with just the IDs that matched and and convert data into a list of dictionaries
            departmentUpdate = departmentNew[departmentNew['department_id'].isin(dep_matched_ids)]
            depData = departmentUpdate.to_dict("records")

            #For loop to upsert data into department table each 1000 records
            for i in range(0, len(depData), 1000):
               depBatch = depData[i:i+1000]

               #Create statement object to insert and update on conflict
               stmt = insert(DepartmentSchema).values(depBatch)
               stmt = stmt.on_conflict_do_update(
                  index_elements=['department_id'],
                  set_={
                     "department": stmt.excluded.department
                  }
               )

               db.session.execute(stmt)
               db.session.commit()
            status = status + "<p>New records inserted into Department Table! - 200</p>"
            
         else:
            status = status + "<p>ERROR: The schema of the new Department CSV to insert, doesn't match the PostgreSQL table schema - 500</p>"
      else:
         status = status + "<p>ERROR: New Department CSV is empty. No data was uploaded for the department table - 500</p>"
   else:
      pass
   
   #Check if the job path with the new CSV file exist
   if os.path.exists(jobPath):
      if os.path.getsize(jobPath) != 0:
         statusCheck = 1
         #Retrieve both historical file and the new file for jobs
         jobNew = pd.read_csv(jobPath, names=["job_id", "job"])
         jobHistorical = pd.read_sql('job', engine)

         #Check if both dataframes have the same schema (Columns and datatypes)
         if jobNew.dtypes.equals(jobHistorical.dtypes):

            #JOIN both dataframes to see the IDs that match and new IDs
            job_matched_ids = tuple(pd.merge(jobHistorical,jobNew, on='job_id',how='right')['job_id'])
            
            #Create new dataframe with just the IDs that matched and convert data into a list of dictionaries
            jobUpdate = jobNew[jobNew['job_id'].isin(job_matched_ids)]
            jobData = jobUpdate.to_dict("records")

            #For loop to upsert data into job table each 1000 records
            for i in range(0, len(jobData), 1000):
               jobBatch = jobData[i:i+1000]

               #Create statement object to insert and update on conflict
               stmt = insert(JobSchema).values(jobBatch)
               stmt = stmt.on_conflict_do_update(
                  index_elements=['job_id'],
                  set_={
                     "job": stmt.excluded.job
                  }
               )

               db.session.execute(stmt)
               db.session.commit()
            status = status + "<p>New records inserted into Job Table! - 200</p>"

         else:
            status = status + "<p>ERROR: The schema of the new Job CSV to insert, doesn't match the PostgreSQL table schema - 500</p>"
      else:
         status = status + "<p>ERROR: New Job CSV is empty. No data was uploaded for the job table - 500</p>"
   else:
      pass

   #Check if the employee path with the new CSV file exist
   if os.path.exists(empPath):
      if os.path.getsize(empPath) != 0:
         statusCheck = 1
         #Retrieve both historical file and the new file for employees
         employeeNew = pd.read_csv(empPath, names=["employee_id", "name", "datetime", "department_id", "job_id"], dtype={'department_id': float, 'job_id': float})
         employeeHistorical = pd.read_sql('employee', engine)

         #Check if both dataframes have the same schema (Columns and datatypes)
         if employeeNew.dtypes[0:3].equals(employeeHistorical.dtypes[0:3]) & ((employeeHistorical.dtypes.department_id in ('float', 'int')) | (employeeHistorical.dtypes.job_id in ('float', 'int'))):

            #Get valid IDs for job_id and department_id from their respective tables
            job_valid_ids = tuple(pd.read_sql('job', engine)['job_id'])
            dep_valid_ids = tuple(pd.read_sql('department', engine)['department_id'])
            
            #Create a dataframe with invalid IDS, ergo, IDs present in the employee CSV as Foreign Keys that don't actually exist in the job and department tables as primary keys 
            emp_invalid = employeeNew[(~employeeNew['job_id'].isin(job_valid_ids) & employeeNew['job_id'].notnull()) | (~employeeNew['department_id'].isin(dep_valid_ids) & employeeNew['department_id'].notnull())]
            
            #If there are invalid IDs, don't insert new employee data
            if emp_invalid.size != 0:
               status = status + "<p>Department ID or Job ID not valid. Foreign key constraint on Employee table not met. Check data and schema - 500</p>"
            #If there are no invalid IDs, insert new employee data
            else:  
               #JOIN both dataframes to see the IDs that match and new IDs 
               employee_matched_ids = tuple(pd.merge(employeeHistorical,employeeNew, on='employee_id',how='right')['employee_id'])

               #Create new dataframe with just the IDs that matched and convert data into a list of dictionaries
               empUpdate = employeeNew[employeeNew['employee_id'].isin(employee_matched_ids)]
               empData = empUpdate.to_dict("records")

               #For loop to upsert data into job table each 1000 records
               for i in range(0, len(empData), 1000):
                  empBatch = empData[i:i+1000]

                  #Create statement object to insert and update on conflict
                  stmt = insert(EmployeeSchema).values(empBatch)
                  stmt = stmt.on_conflict_do_update(
                     index_elements=['employee_id'],
                     set_={
                        "name": stmt.excluded.name,
                        "datetime": stmt.excluded.datetime,
                        "department_id": stmt.excluded.department_id,
                        "job_id": stmt.excluded.job_id
                     }
                  )

                  db.session.execute(stmt)
                  db.session.commit()
               status = status + "<p>New records inserted in Employee table!</p>"
         else:
            status = status + "<p>ERROR: The schema of the new Employee CSV to insert, doesn't match the PostgreSQL table schema</p>"
      else:
         status = status + "<p>ERROR: New Employee CSV is empty. No data was uploaded for the employee table - 500</p>"
   else:
      pass

   if statusCheck == 0:
      finalStatus = iniStatus
   else:
      finalStatus = status
   
   return finalStatus


#End-point = Number of employees hired for each job and department in 2021 divided by quarter. The table must be ordered alphabetically by department and job.
@api.route("/api/v1/number-of-employees", methods = ["GET"])
def number_of_employees():
   #Create PostgreSQL connection
   connection = psycopg2.connect(
      database = pgdatabase,
      user = pguser,
      password = password,
      host = host,
      port = port
   )
   with connection.cursor() as cursor:
      query = sql.SQL("""SELECT de.department, jo.job, 
                              SUM(CASE WHEN CAST(SUBSTRING(em.datetime,6,2) AS INT) BETWEEN 1 AND 3 THEN 1 ELSE 0 END) AS Q1,
                              SUM(CASE WHEN CAST(SUBSTRING(em.datetime,6,2) AS INT) BETWEEN 4 AND 6 THEN 1 ELSE 0 END) AS Q2,
                              SUM(CASE WHEN CAST(SUBSTRING(em.datetime,6,2) AS INT) BETWEEN 7 AND 9 THEN 1 ELSE 0 END) AS Q3,
                              SUM(CASE WHEN CAST(SUBSTRING(em.datetime,6,2) AS INT) BETWEEN 10 AND 12 THEN 1 ELSE 0 END) AS Q4
                        FROM employee AS em 
                        LEFT JOIN department AS de ON em.department_id = de.department_id 
                        LEFT JOIN job AS jo ON em.job_id = jo.job_id
                        WHERE SUBSTRING(em.datetime,1,4) = '2021'
                        GROUP BY de.department, jo.job
                        ORDER BY de.department, jo.job""")
      cursor.execute(query)
      connection.commit()
      rows = cursor.fetchall()
   connection.close()

   #Create basic HTML code to display query results in a table
   result = "<table border='1'><tr><th>DEPARTMENT</th><th>JOB</th><th>Q1</th><th>Q2</th><th>Q3</th><th>Q4</th></tr>"
   for row in rows:
      result = result + f"""<tr><td>{row[0]}</td><td>{row[1]}</td><td>{row[2]}</td><td>{row[3]}</td><td>{row[4]}</td><td>{row[5]}</td></tr>"""
   return result+"</table>"


#End-point = List of ids, name and number of employees hired of each department that hired more employees than the mean of employees 
#            hired in 2021 for all the departments, ordered by the number of employees hired (descending).
@api.route("/api/v1/hired-per-department", methods = ["GET"])
def hired_per_department():
   #Create PostgreSQL connection
   connection = psycopg2.connect(
      database = pgdatabase,
      user = pguser,
      password = password,
      host = host,
      port = port
   )
   with connection.cursor() as cursor:
      query = sql.SQL("""WITH average AS (SELECT AVG(hires) AS average_hires FROM (SELECT department_id, COUNT(*) AS hires 
                                                                                   FROM employee 
                                                                                   WHERE SUBSTRING(datetime,1,4) = '2021' AND department_id IS NOT NULL 
                                                                                   GROUP BY department_id))
                         SELECT em.department_id, de.department, COUNT(em.employee_id) AS hired
                         FROM employee AS em
                         LEFT JOIN department AS de ON em.department_id = de.department_id 
                         GROUP BY em.department_id, de.department
                         HAVING COUNT(em.employee_id) > (SELECT average_hires FROM average)
                         ORDER BY hired DESC;""")
      cursor.execute(query)
      connection.commit()
      rows = cursor.fetchall()
   connection.close()

   #Create basic HTML code to display query results in a table
   result = "<table border='1'><tr><th>ID</th><th>DEPARTMENT ID</th><th>HIRED</th></tr>"
   for row in rows:
      result = result + f"""<tr><td>{row[0]}</td><td>{row[1]}</td><td>{row[2]}</td></tr>"""
   return result+"</table>"


if __name__ == "__main__":
  api.run(host='0.0.0.0', port=5000, debug=True)
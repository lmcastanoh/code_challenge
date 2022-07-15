# Code challenge!

You are a data engineer  and you are about to start an important project. This project is big data migration to a new database system. You need to create a PoC to solve the Requirements

# Solution
This development can be used to integrate data from a google drive source (csv files) to a MySql database located in GCP cloud (Google cloud)

## General description of process
This process was developed in **Python 3.10.4**

I installed packages I did't have because the environment was isolated in this development (working with virtual environments in python). Flask is a little framework to develop the app under MVC model and was used in this project. Aditionally, **GCP Cloud SQL** was used to store the information.

I had 300 free credits in GCP, so those credits were used to create a project and an instance of mySQL in cloud.

Create a folder into my local computer and inizializated the folder as a git repository.
```sh
mkdir code_challenge
```
```git
git init
```
Create a repository into gitHub to conect my local folder with it.

I developed my work on main branch but for a real development process is better to create a branch for each developer and stages of the app to a better manage (for example test, production, versions, etc)

Create a folder to manage my virtual environment.

Install the necessary packages for development such as Flask, pandas, SqlAlchemy, among others.
```sh
mkdir venv
py -m venv venv
```
Start the virtual environment
```sh
source ./venv/Scripts/activate
```
Create a folder named src to save the .py of the functionallity there and create an archive named app.py
```sh
mkdir src
```
## Used packages and initial code

This is the basic code to run an app with Flask

```py
from  flask  import  Flask,request
import  pandas  as  pd
from  sqlalchemy  import  create_engine
import  sqlalchemy
import  requests

app = Flask(__name__)

@app.route('/')
def prueba():
    return ''

if __name__ == "__main__":
    app.run(debug=True)
```
Initialize the application so that it runs locally and define the necessary variables to make the connection with the GCP database: public ip of the mysql instance, the user, the password and the database.

```py
# Create sqlalchemy engine
engine = create_engine("mysql+pymysql://{user}:{pw}@35.227.33.191/{db}"
                       .format(user="root",
                               pw='',
                               db="code_challenge"))
#user = user of database
#pw is the password
#db is the name of the database 
```
In the route **/** I program the function to read the data with the pandas pd.read_csv method and to clean the data that I got, being treated as a dataframe. Some values ​​that are null are filled with a 0 or empty character depending if they are alphanumeric or numeric and I also put a name to the columns of the dataframe.

```py
# Example to read ann clean dataset of jobs
url3 = 'https://drive.google.com/uc?id=1806a7U-HTDSoIMycNOnw5ldC3Gud00li'
jobs = pd.read_csv(url3,header=None)
jobs.columns = ['id','job']
jobs['id'] = jobs['id'].fillna(0)
jobs['job'] = jobs['job'].fillna('')
```

To send the data, I connect to the database through the engine object and its connect method, and through the .to_sql function of SQLAlchemy I send the connection parameters, the dataframe, the way to upload the data (OVERWRITE or INSERT INTO), upload in batch and the data types of each of the columns of the dataframe that is the same type of data that will remain in the MySql instance in Cloud SQL. Then close the connection with the close method and return a database insert success message.
```py
#Create object to the conection
conn = engine.connect()

#Script to insert information into table jobs of the database, this is equivalent to 
# INSERT INTO jobs (id,job) VALUES (1,'Example')

jobs.to_sql('jobs',con=conn,if_exists = 'replace',index=False,chunksize = 1000,
                        dtype = {'id': sqlalchemy.types.INTEGER(),
                                'job': sqlalchemy.types.VARCHAR(length=100)})

#Close the object of the conection
conn.close()
```

*Note:* Errors were not captured for this exercise, but it is always a recommended task to do so to have better control of developments. Some examples:
1. Have a log table to capture the start and end time of the process
2. Capture protocol errors

To run the process, in console execute the following line:

```py
py ./src/app.py
```
This command start the server to listen the client and send data to GCP.

# Challenge #2



####Solution 1
```SQL
SELECT 
IFNULL(b.department,'No Available') AS department,
c.job_d as job,
SUM(CASE WHEN QUARTER(CAST(a.datetime AS DATETIME)) = 1 THEN 1 ELSE 0 END) AS Q1,
SUM(CASE WHEN QUARTER(CAST(a.datetime AS DATETIME)) = 2 THEN 1 ELSE 0 END) AS Q2,
SUM(CASE WHEN QUARTER(CAST(a.datetime AS DATETIME)) = 3 THEN 1 ELSE 0 END) AS Q3,
SUM(CASE WHEN QUARTER(CAST(a.datetime AS DATETIME)) = 4 THEN 1 ELSE 0 END) AS Q4
FROM 
    (SELECT * 
    FROM hired_employees
    WHERE YEAR(CAST(datetime AS DATETIME))=2021) a
LEFT JOIN
    departments b
ON a.department_id = b.id
LEFT JOIN
    (SELECT 
    CASE 
    WHEN REGEXP_INSTR(job, ' I{1}| II{1}| III{1}| IV{1}| V{1}')<>0 THEN SUBSTR(job,1,REGEXP_INSTR(job, ' I{1}| II{1}| III{1}| IV{1}| V{1}')) 
    ELSE job 
    END AS job_d, id 
    FROM jobs) c
ON c.id = a.job_id
GROUP BY b.department,c.job_d
ORDER BY b.department ASC,c.job_d ASC
```

####Solution 2
```SQL
SELECT b.department, COUNT(1) AS hired
FROM (SELECT * 
FROM hired_employees
WHERE YEAR(CAST(datetime AS DATETIME))=2021) a
LEFT JOIN
departments b
ON a.department_id = b.id
LEFT JOIN
(SELECT 
CASE 
WHEN REGEXP_INSTR(job, ' I{1}| II{1}| III{1}| IV{1}| V{1}')<>0 THEN SUBSTR(job,1,REGEXP_INSTR(job, ' I{1}| II{1}| III{1}| IV{1}| V{1}')) 
ELSE job 
END AS job_d, id 
FROM jobs) c
ON c.id = a.job_id
GROUP BY b.department
HAVING COUNT(1) >
    (SELECT AVG(hired) AS media 
    FROM (SELECT b.department, COUNT(1) AS hired
    FROM (SELECT * 
    FROM hired_employees
    WHERE YEAR(CAST(datetime AS DATETIME))=2021) a
    LEFT JOIN
    departments b
    ON a.department_id = b.id
    LEFT JOIN
    (SELECT 
    CASE 
    WHEN REGEXP_INSTR(job, ' I{1}| II{1}| III{1}| IV{1}| V{1}')<>0 THEN SUBSTR(job,1,REGEXP_INSTR(job, ' I{1}| II{1}| III{1}| IV{1}| V{1}')) 
    ELSE job 
    END AS job_d, id 
    FROM jobs) c
    ON c.id = a.job_id
    GROUP BY b.department) media)
```
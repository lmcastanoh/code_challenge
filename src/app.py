from flask import Flask
from flask_mysqldb import MySQL
import pymysql
import pandas as pd
from sqlalchemy import create_engine
import sqlalchemy

app = Flask(__name__)

# create sqlalchemy engine
engine = create_engine("mysql+pymysql://{user}:{pw}@34.71.99.112/{db}"
                       .format(user="root",
                               pw="123456789",
                               db="code_challenge"))

#contexion=MySQL(app)

cnx = pymysql.connect(user='root', password='123456789', host='34.71.99.112', database='code_challenge')
cursor = cnx.cursor()


@app.route('/')
def prueba():
    

    url1 = 'https://drive.google.com/uc?id=1EETspcs5JiotdMxmK32naqVcz3dS03Qy'
    hired_employees = pd.read_csv(url1,header=None)
    hired_employees = pd.DataFrame(hired_employees)
    hired_employees.columns = ['id','name','datetime','department_id','job_id']
    hired_employees['id'] = hired_employees['id'].fillna(0)
    hired_employees['name'] = hired_employees['name'].fillna('')
    hired_employees['datetime'] = hired_employees['datetime'].fillna('')
    hired_employees['department_id'] = hired_employees['department_id'].fillna(0)
    hired_employees['job_id'] = hired_employees['job_id'].fillna(0)

    url2 = 'https://drive.google.com/uc?id=1Z_iMmyJsxiDvr5T0KW9pXBrtqz96BrBG'
    departments = pd.read_csv(url2,header=None)
    departments = pd.DataFrame(departments)
    departments.columns = ['id','department']
    departments['id'] = departments['id'].fillna(0)
    departments['department'] = departments['department'].fillna('')

    url3 = 'https://drive.google.com/uc?id=1806a7U-HTDSoIMycNOnw5ldC3Gud00li'
    jobs = pd.read_csv(url3,header=None)
    jobs = pd.DataFrame(jobs)
    jobs.columns = ['id','job']
    jobs['id'] = jobs['id'].fillna(0)
    jobs['job'] = jobs['job'].fillna('')

    hired_employees.to_sql('hired_employees',con=engine,if_exists = 'replace',index=False,chunksize = 1000,
                           dtype = {'id': sqlalchemy.types.INTEGER(),
                                    'name': sqlalchemy.types.VARCHAR(length=100),
                                    'datetime': sqlalchemy.types.VARCHAR(length=100),
                                    'department_id': sqlalchemy.types.INTEGER(),
                                    'job_id': sqlalchemy.types.INTEGER()})

    departments.to_sql('departments',con=engine,if_exists = 'replace',index=False,chunksize = 1000,
                           dtype = {'id': sqlalchemy.types.INTEGER(),
                                    'department': sqlalchemy.types.VARCHAR(length=100)})

    jobs.to_sql('jobs',con=engine,if_exists = 'replace',index=False,chunksize = 1000,
                           dtype = {'id': sqlalchemy.types.INTEGER(),
                                    'job': sqlalchemy.types.VARCHAR(length=100)})

    return "termine"

if __name__ == "__main__":
    app.run(debug=True)
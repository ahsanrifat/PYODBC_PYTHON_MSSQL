import pandas as pd
import pyodbc

#getting the connection 
def mssql(driver,server,database,uid,password):
    conn = pyodbc.connect('DRIVER={};SERVER={};Database={};UID={};PWD={}'.format(driver,server,database,uid,password),autocommit=True)
    return conn

def mssql_to_csv(conn,query,full_path):
    SQL_Query = pd.read_sql_query(sql_query, conn)
    df= pd.DataFrame(SQL_Query)
    df.to_csv(full_path)

def mssql_to_df(conn,query):
    SQL_Query = pd.read_sql_query(query, conn)
    df= pd.DataFrame(SQL_Query)
    return df


def execute_query(conn,query):
    cursor=conn.cursor()
    cursor.execute(query)

def data_from_query(conn,query):
    cursor=conn.cursor()
    cursor.execute(query)
    rows=cursor.fetchall()
    return rows


#=================MAIN CODE===============================

driver=r"{SQL SERVER}"
server=r""
database=r""
uid=""
password=r""

sql_query=r"SELECT * FROM VW_All_Alarm_February2020"

#provide file's location
file_location=r"E:"

#provide file name without extension
file_name=r"data"

full_path=r"{}\{}.csv".format(file_location,file_name)

conn=mssql(driver,server,database,uid,password)

#example query
create_table="""
create table Battery_Audit(
    idx int not null identity(1,1) primary key,
    SiteCode varchar(100),
    Alarm varchar(200),
    FirstOccurrence varchar(200),
    ClearTimestamp varchar(200),
    Result varchar(100),
    Rule_Number varchar(100)
)
"""
#example query
create_table2="""
create table Battery_Result(
    idx int not null identity(1,1) primary key,
    SiteCode varchar(100),
    SD_start varchar(200),
    MF_start varchar(200),
    Result varchar(100),
    Rule_Number varchar(100)
)
"""

#examples

# execute_query(conn,"delete from battery_audit where rule_number='Rule 1.2'")
# execute_query(conn,"delete from battery_result where rule_number='Rule 1.2'")
# execute_query(conn,create_table)
# print(data_from_query(conn,"select * from Battery_Result"))
print((mssql_to_df(conn,"select * from battery_result where rule_number='Rule 1.2'")))

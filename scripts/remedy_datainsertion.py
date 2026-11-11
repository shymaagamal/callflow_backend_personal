
import paramiko 
import os
import pandas as pd
import sys
from datetime import datetime, timedelta
import pyodbc
import fnmatch
from sqlalchemy import create_engine
import re
import glob




conn_str = 'Driver={ODBC Driver 13 for SQL Server};'\
                        'Server=10.30.30.177;'\
                        'Database=CoreNW_KPIS;'\
                        'UID=sa;'\
                        'PWD=045-145-245v;'


con = pyodbc.connect(conn_str)
cursor = con.cursor()




def readData(local_directory, filenames, flag=None):

    for filename in filenames:
        print(f"File Name is {filename}")
        # list all files that start with the filename pattern
        matching_files = [f for f in os.listdir(local_directory) if f.startswith(filename)]
        if not matching_files:
            print(f"No files found for {filename} in {local_directory}")
            continue

        # pick the latest file by modification time
        latest_file = max(matching_files, key=lambda f: os.path.getmtime(os.path.join(local_directory, f)) )
        file_path = os.path.join(local_directory, latest_file)
        print(f"Loading latest file for {filename}: {file_path}")
 
        print(f"Loaded {latest_file}")
        df = pd.read_csv(file_path)
        df = df[['Incident Number', 'Submit Date', 'Actual Fault Time', 'Actual Resolution Time', 'Status', 'Asset Id',  'Operational Categorization 3', 'Impact', 'Vendor', 'Outage', 'Assigment Status', 'Type', 'Description']]

        df.dropna(subset=['Actual Fault Time'], axis=0, inplace=True)

        str_columns = ['Incident Number', 'Status', 'Operational Categorization 3', 'Impact', 'Vendor', 'Outage', 'Assigment Status', 'Type', 'Description']
        df[str_columns] = df[str_columns].astype(str).fillna('')

        datetime_columns = ['Submit Date', 'Actual Fault Time', 'Actual Resolution Time']
        min_dt = pd.Timestamp("2020-01-01")
        max_dt = pd.Timestamp("2050-12-31")
        for col in datetime_columns:
            df[col] = pd.to_datetime(df[col],  errors='coerce') 
            #df.loc[(df[col] < min_dt) | (df[col] > max_dt), col] = pd.NaT
            #df[col] = df[col].where(df[col].notna(), None)

        print(df.head())

    return df


def insertData(df, flag=None):

    query_1 = ''' INSERT INTO Mobile_Squad.dbo.Remedy_Tickets VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?) '''

    for i, row in df.iterrows():
        try:
            if pd.notna(row['Incident Number']):
                cursor.execute(query_1, (row['Incident Number'],  row['Submit Date'],  row['Actual Fault Time'], row['Actual Resolution Time'] , row['Status'], row['Operational Categorization 3'], row['Impact'], row['Vendor'], row['Outage'], row['Assigment Status'], row['Type'] , row['Description'], row['Asset Id']) )
                con.commit()
        except Exception as e:
            print(str(e))



# local_directory = '/home/CallFlow/BO/Remedy_Tickets'
local_directory = '/data/remedyTkt/'
data = readData(local_directory, ['CE_E2E_REMEDY_TROUBLETICKETS_'], flag=None)
insertData(data, flag=None)
   



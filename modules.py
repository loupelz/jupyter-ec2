# LIBRARIES
import mysql
import config
import gspread
import pymysql
import pandas as pd
import mysql.connector as conn
from mysql.connector import Error
from sqlalchemy import create_engine
from oauth2client.service_account import ServiceAccountCredentials


# connect and initialize variables for gspread (CRM)
scope = ['https://spreadsheets.google.com/feeds',
'https://www.googleapis.com/auth/drive']
creds = ServiceAccountCredentials.from_json_keyfile_name('core-crm-310522-24d337122d7e.json', scope)
client = gspread.authorize(creds)

# function to get data from spreadsheet from Google Drive
def GetSpreadsheetData(sheetName, worksheetIndex):
    sheet = client.open(sheetName).get_worksheet(worksheetIndex)
    return sheet.get_all_values()[1:]


def WriteToMySQLTable(sql_data, tableName):
    
    try:
        connection = mysql.connector.connect(
        user = config.USER,
        password = config.PASSWORD,
        host = config.HOST,
        database = config.DB
        )
        
        sql_drop = " DROP TABLE IF EXISTS {} ".format(tableName)
        
        sql_create_table = """CREATE TABLE {}( 
            user_id INT(20),
            user_firstname TEXT,
            user_lastname TEXT,
            user_email TEXT,
            user_phone VARCHAR(20),
            user_type TEXT, 
            referral TEXT,
            user_nw TEXT,
            user_email_addl TEXT,
            user_phone_addl VARCHAR(20),
            user_addr TEXT,
            user_zip TEXT,
            business_type TEXT,
            user_status TEXT,
            liason TEXT,
            notes TEXT,
            tp TEXT,
            time_log TEXT,
            type TEXT,
            tp1 TEXT,
            time_log_1 TEXT,
            type1 TEXT,
            tp2 TEXT,
            time_log_2 TEXT,
            type2 TEXT,
            PRIMARY KEY (user_id) 
            )""".format(tableName)
 
        sql_insert_statement = """INSERT INTO {}( 
            user_id,
            user_firstname,
            user_lastname,
            user_email,
            user_phone,
            user_type,
            referral,
            user_nw,
            user_email_addl,
            user_phone_addl,
            user_addr,
            user_zip,
            business_type,
            user_status,
            liason,
            notes,
            tp,
            time_log,
            type,
            tp1,
            time_log_1,
            type1,
            tp2,
            time_log_2,
            type2
            )
        VALUES ( %s,%s,%s,%s,%s,%s ,%s ,%s, %s ,%s, %s ,%s,%s ,%s ,%s ,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s   )""".format(tableName)

        cursor = connection.cursor()
        cursor.execute(sql_drop)
        print('Table {} has been dropped'.format(tableName))
        cursor.execute(sql_create_table)
        print('Table {} has been created'.format(tableName))
        
        for i in sql_data:
            cursor.execute(sql_insert_statement, i)
            
        connection.commit()
        print('Table {} successfully updated.'.format(tableName))
        
    except mysql.connector.Error as error :
        connection.rollback()
        print('Error: {}. Table {} not updated!'.format(error, tableName))
        
    finally:
        cursor.execute('SELECT COUNT(*) FROM {}'.format(tableName))
        rowCount = cursor.fetchone()[0]
        print(tableName, 'row count:', rowCount)
        if connection.is_connected():
            cursor.close()
            connection.close()
            print('MySQL connection is closed.')


def PreserveNULLValues(listName):
    print('Preserving NULL values…')
    for x in range(len(listName)):
        for y in range(len(listName[x])):
            if listName[x][y] == '':
                listName[x][y] = None
    print('NULL values preserved.')


data = GetSpreadsheetData('db-schema', 0)
PreserveNULLValues(data)
WriteToMySQLTable(data, 'core_crm')
import mysql.connector
from datetime import datetime, timedelta
from mysql.connector.cursor import MySQLCursor
from mysql.connector import Error
import os

ltEndPoint = "loadtrack-prod-rds.truckertools.com"
ltUserName = "truckstopadmin"
ltPassword = "r0s3G4rd3n"
ltDatabaseName="truckstopadmin"


def movesfLoadTrackingIrregularData(event, context):
    try:
        connection = mysql.connector.connect(user=ltUserName, password=ltPassword, host=ltEndPoint,  database=ltDatabaseName)        
        table_name = 'lt_load_tracking_irregular'
        for record in event['Records']:
            bucket = record['s3']['bucket']['name']
            key = record['s3']['object']['key']
            input_file = os.path.join(bucket,key)
        
        fPath = """s3://{}""".format(input_file)
        print("Bucket {} key {} input file {} final path {}".format(bucket, key, input_file, fPath))
        query = """
                LOAD DATA FROM S3  '{}'
                IGNORE
                INTO TABLE {}
                FIELDS TERMINATED BY '|'
                (loadid, cur_locid, prev_locid, cur_lat, cur_lon, prev_lat, prev_lon, deviceid, phone_number,  
                cur_device_time_gmt, prev_device_time_gmt,
                distance_travelled, time_delta, speed_mph, status_code )
                ;
                """.format(fPath, table_name)
        if connection.is_connected():
            try:
                cursor = connection.cursor()
                print("Execute Load data from {} into {}".format(bucket, table_name))
                cursor.execute(query)
                connection.commit()
            except Error as e:
                print("Error exec load method {} {}".format(e, query))
            finally:
                if cursor is not None:
                    cursor.close()
                connection.close()
    except:
       print('An error occurred in moveloadtracking data.')
      
    
    

import boto3
import time
import os
import pymssql

START_TIME = time.time()
IS_PROD = 0
CUSTOMER_IDS = [614]

# Bucket name from which documents will get deleted
BUCKET_NAME = 'docmanagement-uat'

if IS_PROD == 0:
    ### UAT Config
    SERVER_NAME = '172.25.16.52'
    DB_NAME = 'oha_uat_secondary'
    DB_USER = 'pshealth.admin'
    DB_PASS = 'appian@123'

else:
    ### Prod Config
    SERVER_NAME = '172.25.20.42'
    DB_NAME = 'ohaprodsecondary'
    DB_USER = '***'
    DB_PASS = '***'


def connect(SERVER_NAME,DB_NAME,DB_USER,DB_PASS):
    try:
        conn = pymssql.connect(SERVER_NAME,DB_USER,DB_PASS,DB_NAME)
        return  conn
    except pymssql.OperationalError:
        print("Error in connecting to database")

conn = connect(SERVER_NAME,DB_NAME,DB_USER,DB_PASS)
cursor = conn.cursor()
# s3_upload = boto3.resource('s3')
s3_client = boto3.client('s3')

def insertDocumentS3Metadata(data,conn,cursor):
    insertCase_query = """ 
                            INSERT INTO DCM_Customer_S3ObjectKey(S3ObjectKeyPath,CustomerId,S3ObjectKey)
                            VALUES(%s, %d, %s)
                            """
    cursor.executemany(insertCase_query, data)
    conn.commit()


def customerHandler(customerIds,conn,cursor):
    paginator = s3_client.get_paginator('list_objects')
    truncate_query = "TRUNCATE TABLE DCM_Customer_S3ObjectKey"
    cursor.execute(truncate_query)	
    for customerId in customerIds:
        customerDirPrefix = 'C' + str(customerId)
        pageresponse = paginator.paginate(Bucket=BUCKET_NAME, Prefix=customerDirPrefix)
        S3ObjectPath = []
        S3ObjectKey = []
        CustomerIdList = []
        for pageobject in pageresponse:
            for items in pageobject['Contents']:
                path, filename = os.path.split(items['Key'])
                S3ObjectPath.append(path)
                S3ObjectKey.append(items['Key'])
                CustomerIdList.append(customerId)
        insertDocumentS3Metadata(zip(S3ObjectPath, CustomerIdList,S3ObjectKey), conn,cursor)
        print("Successful")

customerHandler(CUSTOMER_IDS,conn,cursor)


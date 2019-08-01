import boto3
import time
import datetime
import databaseConnectivity as db



IS_PROD = 0
DRY_RUN = 0

START_TIME = time.time()

# Please enter the customer Ids for which you want to perform S3 deletion
CUSTOMER_IDS = [631]

# Bucket name from which documents will get deleted
BUCKET_NAME = 'docmanagement-uat'

# Filepath for the log file
LOGS_DIR = "/data/GitHub/ohassist-s3-data-deletion/Logs"

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

s3_upload = boto3.resource('s3')
s3_client = boto3.client('s3')

# Creates a paginator over all object versions including delete markers
object_response_paginator = s3_client.get_paginator('list_object_versions')

# Creates a paginator over all objects
paginator = s3_client.get_paginator('list_objects')

version_list = []

def logToFile(text,filePath):
    if text:
        with open(filePath, 'a') as logFile:
            print >>logFile, text #for version 2.7.5
            #print(text, end="", file=logFile) ##for version 3.4

def get_Details(customerDirPrefix):
    """
    :param customerDirPrefix: Takes in the prefix of the S3 object Key
    :return: total objects and aggregated size of all objects
    """
    totalObjects  = 0
    size        = 0
    pageresponse = paginator.paginate(Bucket=BUCKET_NAME, Prefix=customerDirPrefix)
    for pageobject in pageresponse:
        if 'Contents' in pageobject.keys():
            for file in pageobject['Contents']:
                totalObjects += 1
                size += file['Size']
    return totalObjects,size


def delete_documents(customerDirPrefix):
    """
    :param customerDirPrefix: Takes in the prefix of the S3 object Key
    :return: None

    Deletes document versions along with the delete markers of the object
    """
    object_response_itr = object_response_paginator.paginate(Bucket=BUCKET_NAME,Prefix=customerDirPrefix)

    for page in object_response_itr:

        # Storing all document versions in the list
        if 'Versions' in page:
            for version in page['Versions']:
                version_list.append({'Key': version['Key'], 'VersionId': version['VersionId']})



    # Deleting all document versions
    for i in range(0, len(version_list), 1000):
        response = s3_client.delete_objects(
            Bucket=BUCKET_NAME,
            Delete={
                'Objects': version_list[i:i+1000],
                'Quiet': True
            }
        )



def customerHandler(customerIds,logsDir):
    conn = db.connect(SERVER_NAME, DB_NAME, DB_USER, DB_PASS)
    for customerId in customerIds:
        # Creating S3 object Key prefix using customerId (e.g - C123)
        customerDirPrefix = 'C'+str(customerId)

        query = 'SELECT CustomerName FROM CUSTOMER where Id = '+ str(customerId)
        queryOutput = db.executeQuery(conn, query)

        try:
            customerName = queryOutput[0][0]
        except IndexError:
            customerName = 'null'

        totalObjects, size = get_Details(customerDirPrefix)

        if DRY_RUN == 1:
            logFileName = customerName+ '_' + 'dry_run'+ '_' + datetime.datetime.now().strftime("%Y%m%d%H%M%S") + '.logs'
        else:
            logFileName = customerName + '_' + datetime.datetime.now().strftime("%Y%m%d%H%M%S")+ '.logs'

        logFilePath = logsDir + '/' + logFileName

        print("Warning : Please check the setting below before proceeding - ")
        print("CUSTOMER_ID : " + str(customerId))
        print("CUSTOMER NAME : " + customerName)
        print("DRY RUN : " + str(DRY_RUN))

        if totalObjects == 0:
            # When no objects are found  for given customerID
            print("No objects found for this Customer ")
        else:
            # Converting the size from bytes to mb
            size  = "{0:.2f}".format(size/ 1024 / 1024)

            if DRY_RUN == 0:
                print("total {0} object(s), of size {1} mb will be deleted for this customer.".format(totalObjects, size))
                print("Warning : Documents once deleted will not be retrieved ")
            else:
                print("total {0} object(s), of size {1} mb present for this customer.".format(totalObjects, size))

            # User confirmation to delete the objects
            user_confirmation = input("Do you really want to proceed with this? -  Yes/No ")

            if user_confirmation == 'Yes':
                if DRY_RUN == 1:
                    message = "total {0} object(s), of size {1} mb to be deleted".format(totalObjects, size)
                    logToFile(message, logFilePath)
                else:
                    delete_documents(customerDirPrefix)
                    message = "total {0} object(s), of size {1} mb are deleted".format(totalObjects, size)
                    logToFile(message,logFilePath)

                print('Please find the log file at - ' + logFilePath + '\n')

            elif user_confirmation == 'No':
                print("No processing will take place. Thank you")
            else:
                print("Invalid Option")


    print('Complete')

customerHandler(CUSTOMER_IDS,LOGS_DIR)

# print("Time taken to complete " + str(time.time()- START_TIME))

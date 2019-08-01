import boto3
import datetime
import sys
reload(sys)
sys.setdefaultencoding('utf8')


DRY_RUN = 1
IS_LOGGING_ENABLED = 1
DAY_THRESHOLD = 30

BUCKET_NAME = 'docmanagement-uat'
LOGS_DIR = "/data/GitHub/ohassist-s3-document-hard-delete/Logs"

s3_client = boto3.client('s3')

# paginator = s3_client.get_paginator('list_objects')
markers_for_deletion = []

# pageresponse = paginator.paginate(Bucket=BUCKET_NAME)

def logToFile(text,filePath):
    if text:
        with open(filePath, 'a') as logFile:
            print >>logFile,  text #for version 2.7.5
	   #print >>logFile, '\n'
            #print(text, end="", file=logFile) ##for version 3.4
            #print('\n', end="", file=logFile)

def removeDeleteMarkers():

    object_response_paginator = s3_client.get_paginator('list_object_versions')
    #object_response_itr = object_response_paginator.paginate(Bucket=BUCKET_NAME,Prefix='C230/')
    object_response_itr = object_response_paginator.paginate(Bucket=BUCKET_NAME)
    logfile = 'S3_HARD_DELETE_' if DRY_RUN == 0 else 'S3_HARD_DELETE_DRY_RUN_'
    logFilePath = LOGS_DIR + '/' + logfile + datetime.datetime.now().strftime("%Y%m%d%H%M%S")


    for page in object_response_itr:
        if 'DeleteMarkers' in page:
            for delete_marker in page['DeleteMarkers']:

                lastModified = delete_marker['LastModified'].replace(tzinfo=None)
                day_diff = datetime.datetime.now() - lastModified
                if day_diff.days>DAY_THRESHOLD:

                    key = delete_marker['Key']
                    message = "Key = {0}, lastModified = {1}".format(key, lastModified)

                    if DRY_RUN == 0:
                        markers_for_deletion.append({'Key': key, 'VersionId': delete_marker['VersionId']})
                        if IS_LOGGING_ENABLED == 1:
                            logToFile(message, logFilePath)
                    else:
                        logToFile(message, logFilePath)
                    # delete_marker_list.append({'Key': delete_marker['Key'], 'lastModified': delete_marker['LastModified'].replace(tzinfo=None)})

            for item in page['Versions']:
                if item['Key'] in [b['Key'] for b in markers_for_deletion]:
                    markers_for_deletion.append({'Key':item['Key'],'VersionId': item['VersionId']})

    for page in object_response_itr:
        if 'Versions' in page:
           for item in page['Versions']:
               if item['Key'] in [b['Key'] for b in markers_for_deletion]:
                   markers_for_deletion.append({'Key':item['Key'],'VersionId': item['VersionId']})

    if DRY_RUN == 0:
        for i in range(0, len(markers_for_deletion), 1000):
            response = s3_client.delete_objects(
                Bucket=BUCKET_NAME,
                Delete={
                    'Objects': markers_for_deletion[i:i+1000],
                    'Quiet': True
                }
            )

    if len(markers_for_deletion)==0:
        message = "No delete marker older than {} day(s) were found.".format(DAY_THRESHOLD)
        logToFile(message, logFilePath)

    print("Complete")
    print("Find the log file at {}".format(logFilePath))

removeDeleteMarkers()


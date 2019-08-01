
import databaseConnectivity as db
import utilityMethods as um
import datetime
import cosmasExtraction as ce
import appianExtraction as ae
import os
# Added
import boto3


#Each run configuration
CUSTOMER_IDS = [641]
APPIAN_OR_COSMAS = 1 # 1-Appian, 2-Cosmas

# Post deployment configuration 
IS_PROD = 0
LOGS_DIR = '/data/GitHub/ohassist-document-extraction/Log Files'


APPIAN_SOURCE_DIR='/data/appian/_admin' # Appian _admin directory path
BATCH_SIZE = 1000                       #Number of documents to be extracted as part of a batch
BATCH = 0
DELAY_TIME = 10                      #Delay time in Seconds
DELAY_AFTER_BATCH = 10                     #Number of batches after which delay will be applied
BUCKET_NAME = 'oha-documentmanagement-poc-1036680' #S3 Bucket Name#


s3 = boto3.client('s3')
s3_upload = boto3.resource('s3')


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
    




def customer_handler():
    
    print("Warning : Please check the setting below before proceeding - ")
    print("IS_PROD : " + ("No" if IS_PROD==0 else "Yes"))
    print("CUSTOMER_IDS : " + str(CUSTOMER_IDS))
    if APPIAN_OR_COSMAS==1:
        print("APPIAN_OR_COSMAS : Appian Extraction") 
    elif APPIAN_OR_COSMAS==2:    
        print("APPIAN_OR_COSMAS : Cosmas Extraction")
    else:
        print("APPIAN_OR_COSMAS : Invalid APPIAN_OR_COSMAS value. It should be either 1 or 2.")
        return   
    
    # Takes in user input
    user_confirmation = input("Do you really want to proceed with this? -  Yes/No ")
    
    
    if  user_confirmation=='Yes' :
        conn = db.connect(SERVER_NAME,DB_NAME,DB_USER,DB_PASS)     
        for i in range(0, len(CUSTOMER_IDS)):    
            customerId = CUSTOMER_IDS[i]
            customerName = um.queryCustomerName(conn,customerId)
            print('\nProcessing Customer - '+customerName)           
             
            logFileName = customerName + '_' + datetime.datetime.now().strftime("%Y%m%d%H%M%S") +('_Appian' if APPIAN_OR_COSMAS==1 else '_Cosmas') +'_ExtractedDocumentLogs.logs'
            logFilePath = LOGS_DIR+'/'+logFileName
            
            
            if APPIAN_OR_COSMAS==1:
                # os.chdir(outputDir)
                query = 'Select top(' + str(BATCH_SIZE) + ') DocumentId, InternalFilePath,ExternalFileName,DocumentS3Path,EmployeeId,CaseId,CustomerId,CaseDocumentId from DCM_vw_AppianNonS3MigratedDocument where CustomerId=' + str(customerId)
                queryOutput = db.executeQuery(conn, query)
                ae.extractAppianDocuments(conn, customerId, 0, logFilePath, APPIAN_SOURCE_DIR, query, queryOutput, BATCH, DELAY_AFTER_BATCH, DELAY_TIME, BUCKET_NAME, s3,s3_upload)

            # Calls the appian document extraction function
            elif APPIAN_OR_COSMAS==2:                
                # os.chdir(outputDir)
                query = 'SELECT TOP(' + str(BATCH_SIZE) + ') * FROM DCM_vw_CosmasNonS3MigratedDocument where CustomerId=' + str(customerId)
                queryOutput = db.executeQuery(conn, query)
                ce.extractCosmasDocuments(conn, customerId, 0, logFilePath, query, queryOutput, BATCH, DELAY_AFTER_BATCH, DELAY_TIME, BUCKET_NAME, s3,s3_upload)
             
            print(customerName + ' customer has been processed.')
            
            print('Please find the log file at - '+logFilePath+'\n\n')
        
        print('Done!!!') 
        conn.close()    
    elif user_confirmation =='No' : 
        print("No processing will take place. Thank you") 
    else : 
        print("Invalid Option")            

customer_handler()   


import utilityMethods as um
import databaseConnectivity as db
import datetime
import os
import time


class documentMetaData:
    def __init__(self,metadataId, employeeIds, caseIds, dateOnDocuments, mimeTypes, documentTypeIds, titles, S3objectKeys,documentIds,IsActive,CreatedBy,CreatedOn,InvalidMetaDataIds,documentNames,documentExtensions):

        self.metadataIds = metadataId
        self.employeeIds = employeeIds
        self.mimeTypes = mimeTypes
        self.caseIds = caseIds 
        self.dateOnDocuments = dateOnDocuments 
        self.documentTypeIds = documentTypeIds 
        self.titles = titles 
        self.S3objectKeys = S3objectKeys
        self.documentIds = documentIds 
        self.IsActive = IsActive 
        self.CreatedBy = CreatedBy 
        self.CreatedOn = CreatedOn
        self.InvalidMetaDataId = InvalidMetaDataIds
        self.documentName = documentNames
        self.documentExtension = documentExtensions
        

# Define addMetaData: Appends data to diiferent properties of documentMetaData class instance
def addMetaData(dm,employeeId,caseId,dateOnDocument,mimeType,documentTypeId,title,S3objectKey,fileName,fileExtension):
    dm.employeeIds.append(employeeId)
    dm.caseIds.append(caseId)
    dm.dateOnDocuments.append(dateOnDocument)
    dm.mimeTypes.append(mimeType)
    dm.documentTypeIds.append(documentTypeId)
    dm.titles.append(title)
    dm.S3objectKeys.append(S3objectKey)
    dm.documentIds.append(-1)
    dm.IsActive.append(0)
    dm.CreatedBy.append('Administrator')
    dm.CreatedOn.append(datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'))
    dm.documentName.append(fileName)
    dm.documentExtension.append(fileExtension)
    return dm

def zipDocumentMetaData(dm):
    return list(zip(dm.caseIds, dm.documentIds, dm.IsActive, dm.documentTypeIds, dm.employeeIds, dm.mimeTypes, dm.dateOnDocuments,
             dm.CreatedBy, dm.CreatedOn, dm.titles, dm.S3objectKeys,dm.documentName,dm.documentExtension))

# 16-01-19 Adding two arguments
def insertDocumentMetadata(data,conn,metadataId,customerId,invalidMetadaId):
    cursor = conn.cursor()
    temp_query = """
    IF OBJECT_ID('tempdb.dbo.#TEST3', 'U') IS NOT NULL
      DROP TABLE #TEST3; 
    CREATE TABLE #TEST3 (
    insertedId int
     );"""
    cursor.execute(temp_query)

    # Storing the generated primary key in the temporary table
    insertCase_query = """ 
                        INSERT INTO OH_CaseDocument(CaseId,DocumentId,IsActive,TypeId,
                        EmployeeId,MIMEType,DateOnDocument,CreatedBy,CreatedDate,Title,
                        S3ObjectKey,DocumentName,Extension,IsExtracted)
                        OUTPUT INSERTED.Id
                        INTO #TEST3
                        VALUES(%d, %d, %d, %s, %d, %s, %s, %s, %s, %s, %d, %s, %s, 1)
                        """
    cursor.executemany(insertCase_query, data)

    cursor.execute("SELECT * FROM #TEST3")
    insertedId = cursor.fetchall()
    insertedId = [i[0] for i in insertedId]
    temp_data = zip(metadataId, insertedId)
    invalidIds = ', '.join(str(i) for i in invalidMetadaId)

    # Creating temporary table for storing document Id and corresponding metadataId
    temp2_query = """
            IF OBJECT_ID('tempdb.dbo.#TEST4', 'U') IS NOT NULL
              DROP TABLE #TEST4;
            CREATE TABLE #TEST4 (
            metadataId int,
            documentId int
             );"""
    cursor.execute(temp2_query)
    cursor.executemany("""INSERT INTO #TEST4 VALUES(%d, %d)""", list(temp_data))
    cursor.execute("""UPDATE dm SET dm.NewSysDocumentId = -t.documentId,dm.LoadedIntoAppian = 1  FROM DM_DocumentsMetadata dm
    JOIN #TEST4 t
    ON dm.Id = t.metadataId
    """)

    cursor.execute(
        "INSERT INTO DCM_S3DocumentObjectKey(DocumentId,S3ObjectKey,Source,CustomerId,DocumentName,Extension) "
        " SELECT -Id,S3ObjectKey,'Cosmas',{0},DocumentName,Extension from OH_CaseDocument where DocumentId = -1 and IsActive = 0".format(customerId)
    )


    cursor.execute(""" UPDATE OH_CaseDocument
                    SET DocumentId = -Id
                    WHERE DocumentId = -1 and IsActive = 0""")

    # Setting NewSysDocumentId = -1 in DM_DocumentsMetadata for invalid documents(which were not found)
    cursor.executemany("""UPDATE DM_DocumentsMetadata 
                    SET NewSysDocumentId = -1
                    WHERE Id in (%s)"""% invalidIds,invalidIds)

    conn.commit()
    
def extractCosmasDocuments(conn,customerId,counter,logFilePath,query,queryOutput,batch, delayAfterBatch, delayTime, bucketName,s3object,s3uploadObject):
    data = queryOutput
    dm = documentMetaData([], [], [], [], [], [], [], [], [], [], [], [], [], [], [])
    batch+=1

    # Iterates over data list for extracting Cosmas documents
    for items in data:        
        #For local testing ---> 
        #if os.path.exists('D:/Doc Mang/Python Jobs'+items[2]):
        # No need to check the path
        if os.path.exists(items[2]):
            # local testing ---> 
            #fileName=um.createStructureCopyFile('D:/Doc Mang/Python Jobs'+items[2],items[8],items[3],-1,conn,customerId)
            # fileName=um.createStructureCopyFile(items[2],items[8],items[3],-1,conn,customerId)

            # Checks for the existing file on S3 and returns fileName and S3Key
            key, fileName = um.getFileName(bucketName=bucketName, s3path=items[8], s3fileName=items[3], s3object=s3object)
            # For local testing 
            # um.uploadDocumentToS3(file='D:/Doc Mang/Python Jobs' + items[2], bucket=bucketName, key=key,s3ObjectUpload=s3uploadObject)

            # Uploads file to S3 if file is not found local system logs it as invalid document
            try:
                um.uploadDocumentToS3(file=items[2], bucket=bucketName, key=key,s3ObjectUpload=s3uploadObject)

                fileSuffix,fileExtension = um.splitFileName(fileName)
                um.logToFile('DocumentPath:'+items[2]+', S3ObjectKey:' +  key+'/'+fileName+'\n', logFilePath)
                dm = addMetaData(dm,items[1],items[4],items[5],items[6],items[7],items[3],key,fileSuffix,fileExtension)
                dm.metadataIds.append(items[0])
            except Exception as e:
                dm.InvalidMetaDataId.append(items[0])
                um.logToFile('DocumentPath:' + items[2] + ', S3ObjectKey:, Document Upload to S3 Failed :  '+e.message+' - ' + items[3] + '\n',
                             logFilePath)
                invalidDocuments = um.InvalidDocuments(customerId, items[1], items[4], -1, '',"Document Upload to S3 Failed : "+e.message+" " + items[2], 'Cosmas')
                um.logInvalidDocuments(conn, invalidDocuments)

            # dm.documentName.append(fileSuffix)
            # dm.documentExtension.append(fileExtension)

        else:
            print('Cosmas Document Not Found - ' + items[2])
            dm.InvalidMetaDataId.append(items[0])
            um.logToFile('DocumentPath:'+items[2]+', S3ObjectKey:, Cosmas Document Not Found - ' + items[3]+'\n', logFilePath)
            invalidDocuments = um.InvalidDocuments(customerId, items[1], items[4], -1, '', 'Document Not Found - ' + items[2], 'Cosmas')         
            um.logInvalidDocuments(conn, invalidDocuments)
    
    dmList = zipDocumentMetaData(dm)
    insertDocumentMetadata(dmList,conn,dm.metadataIds,customerId,dm.InvalidMetaDataId)
    
    
    counter = counter+len(data)
    print("Batch - {0}".format(batch))
    print ('Number of Documents Processed : '+str(counter))

    #Extracting next batch
    data = db.executeQuery(conn, query)
    
    if len(data)==0:
        return
    else :
        if batch % delayAfterBatch == 0:
            print('Waiting')
            time.sleep(delayTime)    #Wait time of 10 seconds after every 10th batch
        extractCosmasDocuments(conn, customerId, counter, logFilePath, query, data, batch, delayAfterBatch, delayTime,bucketName,s3object,s3uploadObject)
        return batch




    

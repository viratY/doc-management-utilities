import utilityMethods as um
import databaseConnectivity as db
import os
import time


# Define updateIsExtracted: Updates OH_CaseDocument table to set
# ISExtracted to 1 and update DocumentName, S3ObjectKey and Extension for given document Ids
def updateCaseDocument(cursor,documentIds):
    cursor.executemany(
        """UPDATE ohc
		    SET 
                ohc.IsExtracted = 1,
                ohc.S3ObjectKey = s3.S3ObjectKey,
                ohc.DocumentName = s3.DocumentName,
                ohc.Extension = s3.Extension
				from OH_CaseDocument ohc
                JOIN DCM_S3DocumentObjectKey s3
             on ohc.DocumentId = s3.DocumentId
        """
        " WHERE ohc.Id in (%s)" % documentIds,
        documentIds
    )

# Define extractAppianDocuments: main method for appian document extraction
def extractAppianDocuments(conn,customerId,counter,logFilePath,sourceDirectory,query,queryOutput,batch, delayAfterBatch, delayTime, bucketName, s3Object, s3uploadObject):
    casedocIds = []
    documentNames = []
    documentExtensions = []
      # Added empty lists
    S3keys = []
    docIds = []
    customerIds = []
    sources = []
    cursor = conn.cursor()
    data = queryOutput
    batch += 1

    # Iterates over data list for extracting appian documents
    for items in data:

        sourcePath = '{0}/{1}'.format(sourceDirectory.replace('\\', '/'), items[1])
        if os.path.exists(sourcePath):
            # fileName = um.createStructureCopyFile(sourcePath,items[3],items[2],items[0],conn,items[6])

            # Checks for the existing file on S3 and returns fileName and S3Key
            key, fileName = um.getFileName(bucketName=bucketName, s3path=items[3], s3fileName=items[2], s3object=s3Object)

            # Uploads file to S3 if file is not found local system logs it as invalid document
            try:
                um.uploadDocumentToS3(file=sourcePath, bucket=bucketName, s3ObjectUpload=s3uploadObject, key=key)
                fileSuffix,fileExension = um.splitFileName(fileName)
                um.logToFile('DocumentPath:'+items[1]+', DocumentId:'+str(items[0])+', S3ObjectKey:' +  items[3]+'/'+fileName+'\n', logFilePath)
                casedocIds.append(items[7])
                documentNames.append(fileSuffix)                #Storing all the filenames
                documentExtensions.append(fileExension)         #Storing all the file Extensions
                docIds.append(items[0])
                customerIds.append(items[6])
                sources.append('Appian')
                S3keys.append(key)
            except Exception as e:
                um.logToFile('DocumentPath:'+items[1]+', DocumentId:'+str(items[0])+', S3ObjectKey:, Document Upload to S3 Failed :  '+e.message+' - ' + items[2]+'\n', logFilePath)
                invalidDocuments = um.InvalidDocuments(customerId, items[5], items[4], items[0], '',"Document Upload to S3 Failed : "+e.message, 'Appian')
                um.logInvalidDocuments(conn, invalidDocuments)
                
        else:
            print('Appian Document Not Found - '+sourcePath)
            um.logToFile('DocumentPath:'+items[1]+', DocumentId:'+str(items[0])+', S3ObjectKey:, Appian Document Not Found - ' + items[2]+'\n', logFilePath)
            invalidDocuments = um.InvalidDocuments(customerId,items[5],items[4],items[0],'','Document Not Found - '+ sourcePath,'Appian')              
            um.logInvalidDocuments(conn, invalidDocuments)
       

    documentIds = ', '.join(str(i) for i in casedocIds)
    
    # print(documentIds)
    cursor.executemany(
        "INSERT INTO DCM_S3DocumentObjectKey(DocumentId,S3ObjectKey,CustomerId,Source,DocumentName,Extension) VALUES (%d, %s, %d, %s, %s, %s)",
        list(zip(docIds, S3keys, customerIds, sources, documentNames, documentExtensions)))

    updateCaseDocument(cursor, documentIds)

    # Setting values of S3objectkey, documentIds, customerIds and sources list to null
    um.setListNull(S3keys, docIds, customerIds, sources)

    conn.commit()
    cursor.close()
    
    counter = counter+len(data)
    print("Batch - {0}".format(batch))
    print ('Number of Documents Processed : '+str(counter))
    #Extracting next batch
    data = db.executeQuery(conn, query)
    
    if len(data)==0:
        return
    else :
        if batch % delayAfterBatch == 0:
            print("waiting")
            time.sleep(delayTime)      #Wait time of 10 seconds after every 10th batch
        extractAppianDocuments(conn, customerId, counter, logFilePath, sourceDirectory, query, data, batch, delayAfterBatch, delayTime, bucketName, s3Object, s3uploadObject)
        return batch

import databaseConnectivity as db
import os
import shutil
import datetime
import re
import sys
# Should be uncommented with Python 2.7
reload(sys)
sys.setdefaultencoding('utf8')
from botocore.errorfactory import ClientError

# Empty list to store S3obectKeys, documentIds, customerIds and source paths
documentIds = []
S3objectKeys = []
customerIds = []
sources = []

# VALID_CHARACTERS holds the valid characters that a file name can have
VALID_CHARACTERS = set('ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz1234567890!-_ .*\'()')

 
class InvalidDocuments:
    def __init__(self, customerId, referralId,employeeId,documentId,createdOn,comment,source):
        self.customerId = customerId
        self.referralId=referralId
        self.employeeId=employeeId
        self.documentId=documentId
        self.createdOn = createdOn
        self.comment=comment
        self.source = source
        
# Define setListNull: takes in variable list arguments and set their values to empty list
def setListNull(*args):
    for arg in args:
        arg[:] = []

def addObjectKeyDocumentId(S3objectKey,documentId,customerId,source):
    documentIds.append(documentId)
    S3objectKeys.append(S3objectKey)
    customerIds.append(customerId)
    sources.append(source)

# Define copyFiles: copies file from the source to the destination
def copyFiles(src,dst,filename,conn):
    destination = os.path.abspath(dst+'/'+filename)
    shutil.copy(src,destination)

# Define createStructureCopyFile: Creates folder structure for a customer and calls copyFiles
def createStructureCopyFile(source,destination,filename,documentId,conn,customerId):
    src = source
    path = destination 
    file = removeInvalidCharacters(filename) 
    file = renameFile(path, file)
   
    if not(os.path.exists(path)):  #If destination doesn't exists create folder
        os.makedirs(path)
        copyFiles(src,path,file,conn)
        addObjectKeyDocumentId(path+'/'+file,documentId,customerId,'Appian')

    else:                          #If destination exists copy files from source
        copyFiles(src, path, file,conn)
        addObjectKeyDocumentId(path+'/'+file, documentId,customerId,'Appian')
    return file

def getFileName(bucketName,s3path,s3fileName,s3object):
    fileName = removeInvalidCharacters(s3fileName)
    fileName = renameFile(fileName, s3object, bucketName, s3path)
    return s3path + '/' +fileName,fileName

def uploadDocument(file,bucket,s3ObjectUpload,key):
    s3ObjectUpload.meta.client.upload_file(file,bucket,key,ExtraArgs={"ServerSideEncryption":"AES256"})

# Define logInvalidDocuments: Create enteries in DCM_InvalidDocuments tables for invalid documents

def logInvalidDocuments(conn,invalidDocuments):
    cursor = conn.cursor()
    cursor.execute("INSERT INTO DCM_InvalidDocuments(CustomerId,ReferralId,EmployeeId,DocumentId,CreatedOn,Comment,Source) VALUES (%d, %d, %d, %d, %s, %s, %s)",(invalidDocuments.customerId, invalidDocuments.referralId, invalidDocuments.employeeId, invalidDocuments.documentId, datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'), invalidDocuments.comment, invalidDocuments.source))
    conn.commit()
    cursor.close()


def queryCustomerName(conn,customerId=None):
    cursor = conn.cursor()
    sql = """Select CustomerName from Customer """ + \
          ' WHERE Id = ' + str(customerId)
               
    cursor.execute(sql)
    data = cursor.fetchall()
    customer_name = ''
    for row in data:
        customer_name = str(row[0])        
    
    cursor.close()   
    return customer_name

# Define logToFile: Creates a log file
def logToFile(text,filePath):
    if text:
        with open(filePath, 'a') as logFile:
            print >>logFile, text #for version 2.7.5
            
# Define rreplace: Replaces the last occurrence 'old' string from string s by 'new'
def rreplace(s, old, new):
    return (s[::-1].replace(old,new, 1))[::-1]

# Define getExtension: Returns the filename and extension from a given file name
def splitFileName(filename):
    
    if os.path.splitext(filename)[1] == '':
        return filename,''
    else:
        dotIndex = filename.rfind('.')
        return filename[:dotIndex],filename[dotIndex + 1:]


# Define checkPath: Return True False based on whether the path exists or not
def checkPath(path,filename):
    pathExists = os.path.exists(r'{0}/{1}'.format(path,filename))
    return  pathExists

def find_between(s, start, end):
    number =(s.split(start))[-1].split(end)[0]
    return rreplace(s,number,str(int(number)+1))

# Define removeInvalidCharacters: Remove invalid characters which are not present in the valid characters list
def removeInvalidCharacters(filename):
    my_new_string = ''.join(filter(VALID_CHARACTERS.__contains__, filename))
    return my_new_string.replace("\\", "")

# Define renameFile: Checks if there exist a file of same name at the given path, If yes renames it
def renameFile(filename,s3,bucketName,s3folderPath):
  
    new_filename = filename
    key = s3folderPath + '/' + filename

    try:
        s3.head_object(Bucket=bucketName, Key=key)
        if not(re.search('_\([0-9]\)$', filename.split('.')[0])):
            if os.path.splitext(new_filename)[1] == '':
                new_filename = new_filename+'_(1)'
            else:
                name, extension = os.path.splitext(new_filename)
                new_filename = filename.replace(filename, name + '_(1)')

            new_filename=renameFile(new_filename,s3,bucketName,s3folderPath)
        else:
            new_filename=find_between(filename,'(',')')
            new_filename=renameFile(new_filename,s3,bucketName,s3folderPath)


    except ClientError:
        pass

    return new_filename

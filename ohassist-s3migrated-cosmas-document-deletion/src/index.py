
# This is Cosmas document deletion job

# import modules

import pymssql
import shutil
import datetime
import os.path
from os import path

# Flip this flag to 1 if running on Prod
IsProd = 0
IsDryRun = 0


IsMigratedEmployee = 1


if IsProd == 0:
    ### UAT Config
    SERVER_NAME = '172.25.16.52'
    DB_NAME = 'oha_uat_secondary'
    DB_USER = 'pshealth.admin'
    DB_PASS = 'appian@123'

    PRIMARY_SOURCE_FOLDER = '/data1/sftp/root/documents/Extract/'
    SECONDARY_SOURCE_FOLDER = '/data1/sftp/root/Documents2/Extract/'
    LOG_FOLDER_NAME = '/data/GitHub/ohassist-s3migrated-cosmas-document-deletion/Log'

    CUSTOMER_IDS = [278]

else:
    ### Prod Config
    SERVER_NAME = '172.25.20.42'
    DB_NAME = 'ohaprodsecondary'
    DB_USER = '***'
    DB_PASS = '***'

    PRIMARY_SOURCE_FOLDER = '/data1/sftp/root/documents/Extract/'
    SECONDARY_SOURCE_FOLDER = '/data1/sftp/root/Documents2/Extract/'
    LOG_FOLDER_NAME = '/data/GitHub/ohassist-s3migrated-cosmas-document-deletion/Log'

    CUSTOMER_IDS = [33]


def countFilesRecursive(dirname):
    cnt = 0
    for file in os.listdir(dirname):
        fullpath = os.path.join(dirname, file)
        if os.path.isdir(fullpath):
            cnt = cnt + countFilesRecursive(fullpath)
        elif os.path.isfile(fullpath):
            cnt = cnt + 1
    return cnt
   
def updateIsExtracted(cursor,customerId):
    cursor.execute(
        """UPDATE DCM_S3DocumentObjectKey
            SET IsDeleted = 1,
                DeletedOn = '%s'
        """ % datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S') +
        " WHERE Source = 'Cosmas' and CustomerId = %s" % customerId,
        customerId
    )

    # cursor.execute( """
    #     update a
    #     set a.IsMigratedToS3 = 1
    #     from
    #     OH_CaseDocument a
    #     inner join DCM_S3DocumentObjectKey b
    #     on a.DocumentId = b.DocumentId AND b.Source = 'Cosmas'
    #     AND ISNULL(a.IsMigratedToS3,'')=''
    #     AND b.CustomerId = %s """% customerId )



def logToFile(text):
    if text:
        with open(LOG_FOLDER_NAME + '/' + LOG_FILE_NAME, 'a') as logFile:
            print >>logFile, text #for version 2.7.5
            #print(text, end="", file=logFile) ##for version 3.4


def queryCustomerName(CustomerId=None):
    cnxn = pymssql.connect(SERVER_NAME, DB_USER, DB_PASS, DB_NAME)

    cursor = cnxn.cursor()

    sql = """Select CustomerName from Customer """ + \
          ' WHERE Id = ' + str(CustomerId)

    cursor.execute(sql)
    data = cursor.fetchall()
    customer_name = ''
    for row in data:
        customer_name = str(row[0])

    cursor.close()
    cnxn.close()
    return customer_name

def queryDocuments(CustomerId=None):
    documentIds = []
    cnxn = pymssql.connect(SERVER_NAME, DB_USER, DB_PASS, DB_NAME)

    cursor = cnxn.cursor()

    sql =""
    if IsMigratedEmployee == 0:
        sql = """SELECT distinct DM.NewSysEmployeeId as EmployeeId, 
            DM.Sub_Contract_Client_Id as EmployeeFolder,
            SourceId  
            FROM DM_DocumentsMetadata DM 
            inner join OH_DD_CustomerContractNameMapping CCNM on CCNM.SubContractName=DM.sub_contract_name """ + \
              ' WHERE CCNM.Customerid = ' + str(CustomerId) + \
              " AND (DM.NewSysEmployeeId is null or DM.NewSysEmployeeId ='')" + \
              ' AND DM.Id NOT IN (3676636, 1783756)' + \
              ' Group by NewSysEmployeeId , Sub_Contract_Client_Id ,SourceId ' + \
              ' ORDER BY Sub_Contract_Client_Id asc'
    else:
        sql = """select distinct [Unique Employee ID] as EmployeeId, 
            SubContractClientId as EmployeeFolder,
            SourceId
           from DCM_vw_DocumentMetadata """ + \
              ' WHERE Customerid = ' + str(CustomerId) + \
              ' AND NewSysDocumentId < 0' + ' AND SubContractClientId IS Not NULL' + \
              ' Group by [Unique Employee ID] , SubContractClientId ,SourceId' + \
              ' ORDER BY [Unique Employee ID] asc'
    cursor.execute(sql)
    data = cursor.fetchall()
    total_deleted_employees = 0
    total_folder_not_found = 0

    # Loop through each row to get the employee source folder
    for row in data:

        employee_id = ('' if row[0] is None else int(row[0]))
        emp_folder = int(0 if row[1] is None else row[1])
        source_id = int(0 if row[2] is None else row[2])

        source_emp_folder = (PRIMARY_SOURCE_FOLDER if source_id == 1 else SECONDARY_SOURCE_FOLDER) + str(emp_folder)
        source_type = ('Primary' if source_id == 1 else 'Secondary')

        logToFile('EmployeeId: ' + str(employee_id) + ', SubContractClientId: ' + str(emp_folder) + '\n')
       
        if path.exists(source_emp_folder):
            total_files_count = countFilesRecursive(source_emp_folder)

            if IsDryRun == 0:
                deleteEmployeeFolder(source_emp_folder)
                total_deleted_employees = total_deleted_employees + 1
                logToFile(str(total_files_count) + ' Total File(s) Deleted From ' + source_type + ' Source\n')
    
            else:
                logToFile(str(total_files_count) + ' Total File(s) to be Deleted From ' + source_type + ' Source\n')
        else:
            total_folder_not_found = total_folder_not_found + 1
            logToFile('Employee Folder Not Found' + '\n')
        logToFile(source_type + ' Path: ' + source_emp_folder + '\n\n')
        
    if IsDryRun == 0:

        updateIsExtracted(cursor,str(CustomerId))

    logToFile('Expected Total Employee Folders to be deleted: ' + str(len(data)) + '\n')
    logToFile('Total Deleted Employee Folders: ' + str(total_deleted_employees) + '\n')
    logToFile('Total Employee Folders Not Found: ' + str(total_folder_not_found))

    cnxn.commit()
    cursor.close()
    cnxn.close()


def deleteEmployeeFolder(source_emp_folder):
    if source_emp_folder is not None:
        shutil.rmtree(source_emp_folder)
        print('Deleted Employee Folder : ' + source_emp_folder)


def customer_handler():
    print("Warning : Please check the setting below before proceeding - ")
    print("IsProd : " + ("No" if IsProd == 0 else "Yes"))
    print("IsDryRun : " + ("No" if IsDryRun == 0 else "Yes"))
    print("IsMigratedEmployee : " + ("No" if IsMigratedEmployee == 0 else "Yes"))
    print("Customer_Ids : " + str(CUSTOMER_IDS) + "\n")
    user_confirmation = input("Do you really want to proceed with this? -  Yes/No ")

    if user_confirmation == 'Yes':
        for i in range(0, len(CUSTOMER_IDS)):
            customerid = CUSTOMER_IDS[i]
            customer_name = queryCustomerName(customerid)
            print('\nProcessing Customer - ' + customer_name)
            global LOG_FILE_NAME
            LOG_FILE_NAME = customer_name + '_' + datetime.datetime.now().strftime("%Y%m%d%H%M%S") + (
                "_Non-Migrated" if IsMigratedEmployee == 0 else "_Migrated") + (
                                '_DeletedCosmasFiles.logs' if IsDryRun == 0 else '_FilesToBeDeletedFromCosmas.logs')
            queryDocuments(customerid)
            print(customer_name + ' customer has been processed.')
            print('Please find the log file at - ' + LOG_FOLDER_NAME + '/' + LOG_FILE_NAME + '\n\n')
    elif user_confirmation == 'No':
        print("No processing will take place. Thank you")
    else:
        print("Invalid Option")


if IsMigratedEmployee == 1:
    customer_handler()
else:
    print("This utility will only run for Migrated employees, please set IsMigratedEmployee = 1")



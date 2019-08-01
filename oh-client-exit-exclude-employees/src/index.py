import os
import shutil

# Importing the database connectivity holding the db connection methods
import databaseConnectivity as db


IS_PROD = 0
FOLDER_DIR = '/tmp/C649/Employees'
CUSTOMER_ID = 649
PARENT_BATCH_ID = 1287
BATCH_SIZE = 1000

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


def extract_employeeIds(parentBatchId,customerId,rownum,batchSize,conn):
    """
    :param parentBatchId: List of client exit batch Ids
    :param customerId: customer Id to which the parent batchId corresponds to
    :param rownum: Id greater than which details will be fetched from the view
    :param batchSize: size of the number of employeeIds to be processed at once
    :param conn: holds the reference to the database connection
    :return:
    """

    # Fetches the employeeIds excluding the employeeIds
    # corresponding to the parentBatchId passed
    query = """
    select top {3} a.Id,a.employeeId 
    from DCM_vw_ClientExitEmployee a
    left join 
    (select employeeid from OH_ClientExit where ParentBatchId={0}) b 
    on b.employeeid=a.employeeid
    where (a.ParentBatchId <> {0} OR ISNULL(a.ParentBatchId,'')='') 
    and a.customerId = {1} and b.employeeid is null
    and a.Id>{2}""".format(parentBatchId,
                         customerId,rownum,batchSize)


    queryOutput = db.executeQuery(conn, query)

    # Appends E prefix to all the fetched employeeIds
    # to create the folder names
    try:
        folderpaths = ['E'+str(employeeId[1]) for employeeId in queryOutput]
        Id = queryOutput[-1][0]
    except IndexError:
        folderpaths,Id = [],0
    return folderpaths,Id


def remove_employee_folders(batchId,customerId,batchSize,folderPath,rownum=0):

        # Establis a database connection
        conn = db.connect(SERVER_NAME, DB_NAME, DB_USER, DB_PASS)

        # holds the folderpaths and last Id fetched in the batch
        folderpaths,rownum = extract_employeeIds(batchId,customerId,rownum,batchSize,conn)

        # Changes the current directory to FOLDER_DIR
        os.chdir(folderPath)

        # Removes all the Employee folders fetched
        for folder in folderpaths:
            try:
                shutil.rmtree(os.getcwd()+'/'+folder)
            except OSError:
                continue

        # Checks if there are more to process if no  closes the database connection
        if len(folderpaths) ==0:
            conn.close()
            return
        else:
        # If there are more employee folders repeats the process
            remove_employee_folders(batchId=batchId,customerId=customerId,batchSize=batchSize,folderPath=folderPath,rownum=rownum)
            return rownum

def main_handler(batchId,customerId,batchSize,folderPath):
    print("Warning : Please check the setting below before proceeding - ")
    print("IS_PROD : " + ("No" if IS_PROD == 0 else "Yes"))
    print("CUSTOMER_IDS : " + str(customerId))
    print("BATCH_IDS : " + str(batchId))
    print("BATCH_SIZE : " + str(batchSize))
    print("FOLDER_DIR : " + str(folderPath))

    user_confirmation = input("Do you really want to proceed with this? -  Yes/No ")

    if user_confirmation == 'Yes':
        remove_employee_folders(batchId=batchId,customerId=customerId,batchSize=batchSize,folderPath=folderPath)
        print('Done!!!')
    elif user_confirmation == 'No':
        print("No processing will take place. Thank you")
    else:
        print("Invalid Option")

main_handler(batchId=PARENT_BATCH_ID,customerId=CUSTOMER_ID,batchSize=BATCH_SIZE,folderPath=FOLDER_DIR)

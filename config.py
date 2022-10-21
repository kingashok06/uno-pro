# S3_BUCKET = "kashok"
# ftp credentials
SFTP_HOST     = "127.0.0.1"
SFTP_PORT     = 22
SFTP_USERNAME = "dev25"
SFTP_PASSWORD = "data@123"

# s3 parameters
S3_BUCKET = "s3://kashok"

#MASTERCARD_250_DATA_FILE
BINARY_PARENT_DIRECTORY   = "jsonfile/downloaded_sftp"
BINARY_FILE_DIRECTORY     = "jsonfile/downloaded_sftp/B.OTHERS_FILES/464"
CONVERTED_BINARY_CSV_FILE = "jsonfile/download_csv"
MASTERCARD_DOWNLOAD_SFTP  = "/home/dev25/A.HPS_TO_UNO1"

#STG FILES OF NON_PROD
STG_DIRECTORY               = 'jsonfile/downloaded_stg'
STG_CONVERTED_CSV_DIRECTORY = 'jsonfile/converted_csv'
STG_DOWNLOAD_SFTP           = '/home/dev25/A.HPS_TO_UNO/B.OTHERFILES/16102022/'

#DATABASE_DETAIL
DATABASE    =   "new_db"
HOST        =   "localhost"
USER        =   "postgres"
PASSWORD    =   "postgres"
PORT        =   "5432"
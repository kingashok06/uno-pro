import os
import csv
import psycopg2

import paramiko
import pandas as pd
import config as cfg

from stat import S_ISDIR, S_ISREG
from datetime import date, timedelta, datetime
from pathlib import Path
import shutil
import time


class SFTPIngestion:

    def __init__(self):
        self.host = cfg.HOST
        self.port = cfg.PORT
        self.user = cfg.USER
        self.database = cfg.DATABASE
        self.password = cfg.PASSWORD
        self.root_path = cfg.ROOT_PATH
        self.sftp_host = cfg.SFTP_HOST
        self.sftp_port = cfg.SFTP_PORT
        self.sftp_username = cfg.SFTP_USERNAME
        self.sftp_password = cfg.SFTP_PASSWORD
        self.s3_bucket_name = cfg.S3_BUCKET
        self.stg_directory = cfg.STG_DIRECTORY
        self.binary_parent_directory = cfg.BINARY_PARENT_DIRECTORY
        self.binary_file_directory = cfg.BINARY_FILE_DIRECTORY
        self.converted_binary_csv_file = cfg.CONVERTED_BINARY_CSV_FILE
        self.stg_converted_csv_directory = cfg.STG_CONVERTED_CSV_DIRECTORY

    def create_sftp_connection(self):
        """
        Creates an SFTP connection from the ssh connection created wiht the FTP server.
        :return : obj. connection object
        """
        transport = paramiko.Transport((self.sftp_host, self.sftp_port))
        transport.connect(
            username=self.sftp_username,
            password=self.sftp_password
        )

        sftp = paramiko.SFTPClient.from_transport(transport)
        print("connection created")
        return sftp

    @staticmethod
    def sftp_get_recursive(path, dest, sftp):
        """
        This method help us to download files from sftp server
        """
        item_list = sftp.listdir_attr(path)
        dest = str(dest)
        if not os.path.isdir(dest):
            os.makedirs(dest, exist_ok=True)
        for item in item_list:
            mode = item.st_mode
            if S_ISDIR(mode):
                SFTPIngestion.sftp_get_recursive(path + "/" + item.filename,
                                                 dest + "/" + item.filename, sftp)
            else:
                sftp.get(path + "/" + item.filename,
                         dest + "/" + item.filename)

    def creating_direcotries(self):
        """
        This method is going to create the respective direcotries for us
        """
        dir_list = ['converted_csv', 'download_csv',
                    'downloaded_sftp', 'downloaded_stg']

        for items in dir_list:
            if os.path.exists(os.path.join(self.root_path, items)):
                continue
            else:
                path = os.path.join(self.root_path, items)
                os.mkdir(path)

    def converting_mastercard_report(self):
        """
        This method is going to convert the Mastercard report files to CSV format
        """
        try:
            cloumns_list = ['Message Type Ind', 'Switch sr', 'Acquirer/ issuer',	'Processor ID',	'Txn Date',	'Txn Time',	'PAN Length',	'PAN #',	'Proc code',	'Trace#',	'Mechant Type',	'PoS Entry',	'Ref#',	'Acqirer Inst ID',	'Terminal ID',	'Response Code',	'Brand', 'Advise Reason code',	'Intra CCY Agreement Code',	'Auth ID',	'CCY Code-Txn',	'Implied Dec - Txn',	'Competed Amt Txn- Local', 'Competed Amt Txn - Local DR/ CR indicator',	'CashBack Amt Local',	'Cashback Amt Txn - Local DR/ CR indicator',	'Access Fee Local',
                            'Access Fee - Local DR/ CR indicator',	'CCY Code -Settlement',	'Implied Decimal - Settlement',	'Conversion Rate Settlement',	'Completed Amt Settlement',	'Completed Amt Settlement -n Dr/Cr Indicator',	'Interchange fee',	'Interchange fee -n Dr/Cr Indicator',	'Service level indicator',	'Response code',	'Filler', 'Positive id indicator', 'ATM surcharge fee program ID',	'Crossborder indicator',	'Crossborder currency indicator',	'Filler',	'Requested amt trans: local',	'Filler',	'Trace number: adjustment trans',	'Recon activity']
            for filename in os.listdir(self.binary_file_directory):
                f = os.path.join(self.binary_file_directory, filename)
                new_csv_filename = f.replace('.001', '.csv')
                new_csv = new_csv_filename.replace(
                    self.binary_file_directory, self.converted_binary_csv_file)
                if os.path.isfile(f) and filename.endswith('.001'):
                    with open(f, 'r') as f:
                        nrec_list = [cloumns_list]
                        with open(f'{new_csv}', mode="w") as outfile:
                            for value in f.readlines():  # read all lines
                                if value.startswith('NREC') or value.startswith('FREC'):
                                    values = value[0:4], value[4:13], value[13], value[14:18], value[18:24], value[24:30], value[30:32], value[32:50], value[50:57], value[57:63], value[63:67], value[67:70], value[70:82], value[82:92], value[92:102], value[102:104], value[104:107], value[107:114], value[114:118], value[118:124], value[124:127], value[127], value[128:
                                                                                                                                                                                                                                                                                                                                                                            140], value[140], value[141:153], value[153], value[154:162], value[162], value[163:166], value[166], value[167:175], value[175:187], value[187], value[188:198], value[198], value[199:202], value[202:204], value[204:214], value[214], value[215], value[216], value[217], value[218], value[219:231], value[231:243], value[243:249], value[249]
                                    con_list = list(values)
                                    nrec_list.append(con_list)
                            writer = csv.writer(outfile)
                            writer.writerows(nrec_list)
        except Exception as ex:
            return ex

    def converting_stg_files(self):
        """
        This method is going to convert the stg files to CSV format
        """
        try:
            for filename in os.listdir(self.stg_directory):
                print(filename)
                f = os.path.join(self.stg_directory, filename)
                if os.path.isfile(f) and filename.endswith('.STG'):
                    with open(f"{f}", mode="r") as infile:
                        reader = csv.reader(infile,  delimiter='|')
                        csv_variable = f.replace('.STG', '.csv')
                        new_csv = csv_variable.replace(
                            self.stg_directory, self.stg_converted_csv_directory)
                        if os.path.exists(new_csv):
                            continue
                        else:
                            with open(f"{new_csv}", mode="w") as outfile:
                                writer = csv.writer(outfile, delimiter=',')
                                writer.writerows(reader)
        except Exception as ex:
            return ex

    def upload_csv_to_s3(self):
        """
        THis method will upload the converted csv file to the destined s3 path. This method develops the string that will be used when uploading to the s3. The files are
        uploaded to the specified partition.
        The partition structure is as follows:
        current_year, current_month,current_day, current_hour
        """
        current_date = datetime.now()
        current_year = str(current_date.year)
        current_month = str(current_date.month)
        current_day = str(current_date.day)
        current_hour = str(current_date.hour)
        DATE = "%s-%s-%s::%s" % (current_year, current_month,
                                 current_day, current_hour)

        print("aws s3 sync " + self.stg_converted_csv_directory +
              " " + f"{self.s3_bucket_name}/{DATE}/stg/")
        is_upload_success = os.system(
            "aws s3 sync " + self.stg_converted_csv_directory + " " + f"{self.s3_bucket_name}/{DATE}/stg/")
        is_upload_success = os.system(
            "aws s3 sync " + self.converted_binary_csv_file + " " + f"{self.s3_bucket_name}/{DATE}/binary/")
        print(is_upload_success)
        if is_upload_success == 0:
            print("Uploaded " + self.stg_converted_csv_directory +
                  " to " + self.s3_bucket_name)
            print("Uploaded " + self.converted_binary_csv_file +
                  " to " + self.s3_bucket_name)
            # os.system("rm " + self.stg_converted_csv_directory )
        else:
            print("Upload of file " + self.stg_converted_csv_directory + " failed")

    def close_connections(self):
        """
        This mehtod is used to close the ssh and sftp connections.
        """
        print('closing connections')
        self.create_sftp_connection().close()

    @staticmethod
    def card_prod_dimension(empdata, empdata_columns):
        """
        This mehtod create the update set string that is going to be placed in the upsert query.
        return : string , an updated string value
        """
        update_set = ''
        for i in range(1, len(empdata_columns)):
            update_set += empdata_columns[i] + \
                '=EXCLUDED.' + empdata_columns[i]+" "
        update_set = update_set.strip()
        update_set = update_set.replace(' ', ',')
        return update_set

    def injecting_data_to_ods(self):
        """
        This mehtod is going to inject the data of csv files into the ods with the hep of upser query
        """
        connection = psycopg2.connect(
            database=cfg.DATABASE,
            host=cfg.HOST,
            user=cfg.USER,
            password=cfg.PASSWORD,
            port=cfg.PORT
        )
        try:
            cursor = connection.cursor()
            stg_converted_csv_directory = 'jsonfile/temp/converted_csv'
            for filename in os.listdir(stg_converted_csv_directory):
                f = os.path.join(stg_converted_csv_directory, filename)
                if os.path.isfile(f) and filename.endswith('.csv'):
                    filename = filename.replace('.csv', "")
                    filename = ''.join(c if c not in map(str, range(
                        0, 10)) else "" for c in filename).replace('--', "").replace('_', '')
                    ods_data = pd.read_csv(f, index_col=False, delimiter=',')
                    ods_data = ods_data.astype(object).where(
                        pd.notnull(ods_data), None)
                    ods_data.columns = ods_data.columns = ods_data.columns.str.replace(
                        r' ', '', regex=True).str.replace(r'-', '', regex=True).str.replace(r'_', '', regex=True)
                    ods_data_columns = ods_data.columns.tolist()
                    values_string = (', '.join(['%s'] * len(ods_data_columns)))
                    update_set = self.card_prod_dimension(
                        ods_data, ods_data_columns)
                    for i, row in ods_data.iterrows():
                        query = """ INSERT INTO   {0}
                                    VALUES       ({1})
                                    ON CONFLICT  ({2})
                                    DO UPDATE SET {3}
                                                    """.format(filename, values_string, ods_data_columns[0], update_set)
                        cursor.execute(query, tuple(row))
                    print("Record inserted successfully into table")
            connection.commit()
            count = cursor.rowcount
            print(count, "Record inserted successfully into table")
        except (Exception, psycopg2.Error) as error: 
            print(count, "Failed to insert record into table", error)
        finally:  # closing database connection.
            if connection:
                cursor.close()
                connection.close()
                print("PostgreSQL connection is closed")

    def delete_locally_stored_file(self):
        """
        This mehtod Delete files downloaded from the non-prod server --The stg files
        Delete files downloaded from the prod server --The 250-data-binary files
        Delete Converted stg CSV files
        Delete Converted 250-Data-Binary file
        """
        for mc_files in os.listdir(self.root_path): 
            dirpath = Path(self.root_path) / mc_files
            if dirpath.exists() and dirpath.is_dir():
                shutil.rmtree(dirpath)
        start_time = time.time()
        print("--- %s seconds ---" % (time.time() - start_time))
        # parent_directory = 'jsonfile/temp'
        # for files in os.listdir(parent_directory):
        #     # if os.path.isdir(os.path.join(parent_directory,files)):
        #     for new_files in os.listdir(os.path.join(parent_directory, files)):
        #         if os.path.join(parent_directory, files) == self.stg_directory or os.path.join(parent_directory, files) == self.stg_converted_csv_directory or os.path.join(parent_directory, files) == self.converted_binary_csv_file:
        #             os.remove(os.path.join(parent_directory, files, new_files))

    def initiateIngestion(self):
        yesterday_date = date.today() - timedelta(days=1)
        yesterday = yesterday_date.strftime('%d%m%Y')

        stg_object = self.create_sftp_connection()
        self.creating_direcotries()
        self.sftp_get_recursive(cfg.MASTERCARD_DOWNLOAD_SFTP,
                                cfg.BINARY_PARENT_DIRECTORY, stg_object)
        self.sftp_get_recursive(cfg.STG_DOWNLOAD_SFTP,
                                cfg.STG_DIRECTORY, stg_object)
        self.converting_mastercard_report()
        self.converting_stg_files()
        # self.upload_csv_to_s3()
        # self.injecting_data_to_ods()
        self.delete_locally_stored_file()


if __name__ == "__main__":
    ftp_obj = SFTPIngestion()
    stg_object = ftp_obj.initiateIngestion()

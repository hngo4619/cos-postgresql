"""
   Copyright 2019 Huy Ngo

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
"""

#!/usr/bin/python
import boto3
import csv
import ibm_boto3
from ibm_botocore.client import Config,ClientError
import os
import psycopg2
import time
import datetime
import logging
from configparser import ConfigParser
import argparse

################################ Argument Parser ######################################
parser  =   argparse.ArgumentParser(description='Supply config file and do Export or Import based on input')
parser.add_argument('config',type=str, help="Configuration file")
parser.add_argument('--delete', action='store_true', help="Use this argument to delete the local file after importing/exporting")

flag = parser.add_mutually_exclusive_group(required=True)
flag.add_argument('--i', action='store_true', help="Use this argument to do the import process using provided configuration file")
flag.add_argument('--e', action='store_true', help="Use this argument to do the export process using provided configuration file")

storage = parser.add_mutually_exclusive_group(required=True)
storage.add_argument('--local', action='store_true', help="Use this argument to import from local file")
storage.add_argument('--ibm', action='store_true', help="Use this argument to import from IBM COS")
storage.add_argument('--aws', action='store_true', help="Use this argument to import from AWS S3")

args    =   parser.parse_args()


################################ Common Methods for Import and Export ##########################################
def config(file=args.config, section=''):
    """ Read section from configuration file """
    parser = ConfigParser()
    parser.read(file)

    config = {}
    if parser.has_section(section):
        params = parser.items(section)
        for param in params:
            config[param[0]] = param[1]
    else:
        raise Exception('Unable to find section in config file')

    return config


def connect():
    """ Connect to the PostgreSQL database server """
    conn = None
    try:
        # read connection parameters
        params = config(section='postgresql')

        # connect to the PostgreSQL server
        print('Connecting to the PostgreSQL database.')
        conn = psycopg2.connect(**params)

        # create a cursor
        cur = conn.cursor()
        print('Connected to PostgreSQL database.')
        return cur, conn
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)


############################################# Export methods #################################################
def read_schema():
    """ Loop through all tables in schema to do the export """
    params = config(section='table')
    # Query to get table names from the given schema
    str_select = """SELECT table_name FROM information_schema.tables where table_schema='{}'""".format(params.get('schema'))
    try:
        # Execute query
        cur.execute(str_select)
        tables = cur.fetchall()
        print("Starting Export......")
        if (len(tables)  ==   0 ):
            print("No tables found in the give schema..Exiting....")
            exit
        else:
            print("Found {} tables in the schema".format(len(tables)))
            for table in tables:
                export(table[0]) # for each table, call the export method
        print("Data export successful !!!")
    except (Exception, psycopg2.Error) as error :
        print(error)


def export(table):
    """ Write data from table data to a file """
    local = config(section='local')
    filename = table+'.csv' # Files are named after corresponding tables
    file    =   local.get('path') + '/' + filename
    # Execute query
    str_select = """SELECT * FROM {}""".format(table)
    cur.execute(str_select)
    # Fetch the data returned
    results = cur.fetchall()
    # Extract the table headers
    # headers = [i[0] for i in cur.description]

    # Open CSV file for writing.
    csvFile = csv.writer(open(file , 'w', newline=''),
                            delimiter='', lineterminator='\r\n')
                            #quoting=csv.QUOTE_ALL, escapechar='\\')
    # Add headers to the CSV file.
    # csvFile.writerow(headers)

    # Add data to the CSV file.
    csvFile.writerows(results)
    # Message stating export successful.
    print("Data export successful from table - ",table)
    print("Local file created - ",file)
    upload(filename)

    # Delete local file after upload
    cleanup_after_export(file)


def cleanup_after_export(file):
    """ Delete local files """
    if(args.delete):
        try:
            os.remove(file)
            print("Deleted local file - ", file)
        except OSError:
            pass


def upload(filename):
    """ Upload the CSV to S3 or COS """
    aws = config(section='aws')
    ibm = config(section='ibm')
    local = config(section='local')
    if args.aws and aws.get('access_key_id').strip() and aws.get('secret_access_key').strip() and aws.get('bucket').strip():
        s3 = boto3.client(
            's3',
            aws_access_key_id=aws.get('access_key_id'),
            aws_secret_access_key=aws.get('secret_access_key'),
        )

        s3.upload_file(local.get('path') + '/' + filename , aws.get('bucket'), filename)
        print("File Uploaded to AWS S3 - ", filename)
    elif args.ibm and ibm.get('api_key_id').strip() and ibm.get('instance_id').strip() and ibm.get('auth_endpoint').strip() and ibm.get('endpoint').strip() and ibm.get('bucket').strip():

        # IBM COS
        cos = ibm_boto3.resource("s3",
            ibm_api_key_id=ibm.get('api_key_id'),
            ibm_service_instance_id=ibm.get('instance_id'),
            ibm_auth_endpoint=ibm.get('auth_endpoint'),
            config=Config(signature_version="oauth"),
            endpoint_url=ibm.get('endpoint')
        )

        cos.meta.client.upload_file(local.get('path') +  '/' + filename ,ibm.get('bucket'), filename)
        print("File Uploaded to IBM COS - ", filename)
    elif args.local:
        print("--local flag is not valid for export!!! To upload to COS or S3 choose --ibm or --aws respectively")
    else:
        print("Details are missing to upload through AWS/IBM COS")

################################## Import methods ########################################
def cleanup(cur, conn):
    params = config(section='local')
    """ Delete temp file if exists """
    try:
        os.remove("tmp_csv.csv")
        print("Temp file deleted.")
    except OSError:
        pass

    """ Delete local file if passed in """
    if(args.delete):
        try:
            os.remove('{}/{}'.format(params.get('path'), params.get('filename')))
            print("Local file deleted.")
        except OSError:
            pass

    """ Close the database connection """
    cur.close()
    conn.close()
    print('Database connection closed.')


def create(cur, conn):
    """ Check if table needs to be created """
    params = config(section='table')
    columns = params.get('columns').split(',')

    str_exists = """SELECT to_regclass('{}.{}');""".format(params.get('schema'), params.get('table'))
    cur.execute(str_exists)

    if cur.fetchone()[0] is None:
        print("Table doesn't exist.")
        str_create = """CREATE TABLE IF NOT EXISTS {}.{} (\n""".format(params.get('schema'), params.get('table'))

        for col in columns[:-1]:
            str_create += "\t{} VARCHAR(255), \n".format(col)

        str_create += "\t{} VARCHAR(255) \n);".format(columns[-1])

        cur.execute(str_create)
        conn.commit()
        print("Table created.")
    else:
        print("Table already exists.")


def download():
    """ Download the CSV from S3 or COS """
    aws = config(section='aws')
    ibm = config(section='ibm')
    local = config(section='local')

    if args.local and local.get('path').strip() and local.get('filename').strip():
        print("Local file located.")
        upload("{}/{}".format(local.get('path'), local.get('filename')))
    elif args.aws and aws.get('access_key_id').strip() and aws.get('secret_access_key').strip() and aws.get('bucket').strip() and aws.get('filename').strip():
        client = boto3.client(
            's3',
            aws_access_key_id=aws.get('access_key_id'),
            aws_secret_access_key=aws.get('secret_access_key'),
        )

        client.download_file(aws.get('bucket'), aws.get('filename'), 'py_download_tmp_csv.csv')
        print("File downloaded from AWS S3.")
        upload("py_download_tmp_csv.csv")
    elif args.ibm and ibm.get('api_key_id').strip() and ibm.get('instance_id').strip() and ibm.get('auth_endpoint').strip() and ibm.get('endpoint').strip() and ibm.get('bucket').strip() and ibm.get('filename').strip():
        s3 = ibm_boto3.resource("s3",
            ibm_api_key_id=ibm.get('api_key_id'),
            ibm_service_instance_id=ibm.get('instance_id'),
            ibm_auth_endpoint=ibm.get('auth_endpoint'),
            config=Config(signature_version="oauth"),
            endpoint_url=ibm.get('endpoint')
        )

        s3.meta.client.download_file(ibm.get('bucket'), ibm.get('filename'), 'py_download_tmp_csv.csv')
        print("File downloaded from IBM COS.")
        upload('py_download_tmp_csv.csv')
    else:
        print("Error: CSV file not provided locally or through AWS/IBM COS")


def insert(file):
    """ Insert data into PostgreSQL """
    params = config(section='table')
    str_insert = """INSERT INTO {}.{}\nVALUES """.format(params.get('schema'), params.get('table'))

    with open(file, 'r') as csv_file:
        csv_reader = csv.reader(csv_file, delimiter='')
        csv_list = list(csv_reader)

        for row in csv_list[:-1]:
            str_insert += "("
            for data in row[:-1]:
                str_insert += "'{}',".format(data)
            str_insert += "'{}'".format(row[-1])
            str_insert += "),\n"

        str_insert += "("
        for data in csv_list[-1][:-1]:
            str_insert += "'{}',".format(data)
        str_insert += "'{}'".format(csv_list[-1][-1])
        str_insert += ");"
    print("CSV file parsed.")

    cur.execute(str_insert)
    conn.commit()
    print("Data inserted into PostgreSQL.")


if __name__ == '__main__':
    print("Hello.")

    print(args.config)
    start = time.time()
    cur, conn = connect()

    if (args.e):
        print("Export arg supplied")
        read_schema()
        """ Close the database connection """
        cur.close()
        conn.close()
        print('Database connection closed.')


    if (args.i):
        print("Import arg supplied")
        create(cur, conn)
        download()
        cleanup(cur, conn)

    end = time.time()
    print("Total time: {}".format(end - start))
    print("Goodbye.")

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
import os
import psycopg2
import time
from configparser import ConfigParser
from ibm_botocore.client import Config, ClientError

def cleanup(cur, conn):
    """ Delete temp file if exists """
    try:
        os.remove("tmp_csv.csv")
        print("Temp file deleted.")
    except OSError:
        pass

    """ Close the database connection """
    cur.close()
    conn.close()
    print('Database connection closed.')

def config(file='database.ini', section=''):
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

    if local.get('path').strip():
        print("Local file located.")
        upload("{}/{}".format(local.get('path'), local.get('filename')))
    else:
        client = boto3.client(
            's3',
            aws_access_key_id=aws.get('access_key_id'),
            aws_secret_access_key=aws.get('secret_access_key'),
        )

        client.download_file(aws.get('bucket'), aws.get('filename'), 'tmp_csv.csv')
        print("File downloaded from AWS S3.")
        upload("tmp_csv.csv")

        # s3 = ibm_boto3.resource("s3",
        #     ibm_api_key_id=ibm.get('api_key_id'),
        #     ibm_service_instance_id=ibm.get('instance_id'),
        #     ibm_auth_endpoint=ibm.get('auth_endpoint'),
        #     config=Config(signature_version="oauth"),
        #     endpoint_url=ibm.get('endpoint')
        # )
        #
        # s3.meta.client.download_file('accolade-hdngo-001', 'test_processed.csv', './tmp.csv')


def upload(file):
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
    start = time.time()

    cur, conn = connect()
    create(cur, conn)
    download()
    cleanup(cur, conn)

    end = time.time()
    print("Total time: {}".format(end - start))
    print("Goodbye.")

#!/usr/bin/python
import boto3
import csv
import ibm_boto3
import os
import psycopg2
import time
from configparser import ConfigParser

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

def config(filename='database.ini', section=''):
    parser = ConfigParser()
    parser.read(filename)

    db = {}
    if parser.has_section(section):
        params = parser.items(section)
        for param in params:
            db[param[0]] = param[1]
    else:
        raise Exception('Section {0} not found in the {1} file'.format(section, filename))

    return db

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

        # cos = ibm_boto3.client(
        #     service_name='s3',
        #     ibm_api_key_id=ibm.get('api_key_id'),
        #     ibm_service_instance_id=ibm.get('instance_id'),
        #     ibm_auth_endpoint=ibm.get('auth_endpoint'),
        #     endpoint_url=ibm.get('endpoint')
        # )
        #
        # cos.Object(ibm.get('bucket'), ibm.get('filename')).download_file('./tmp.csv')

        # cos.download_file(Bucket=ibm.get('bucket'), Key=ibm.get('filename'), Filename='ibm_tmp_csv.csv')


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

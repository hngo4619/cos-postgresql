# DISCLAIMER
This code is not designed for production. If you want to use it for production purposes, it is your responsibility to implement security measures as needed. The author or contributors to this repo will not be liable for any direct, indirect, incidental, special, exemplary, or consequential damages (including, but not
limited to, procurement of substitute goods or services; loss of use,
data, or profits; or business interruption). Use it at your own risk!


# Inserting a CSV file to PostgreSQL

## pgfutter
pgfutter is a CLI tool to import CSV and JSON into PostgreSQL. To download and look at the README, head to the [GitHub repo](https://github.com/lukasmartinelli/pgfutter).

After installation, you can move the pgfutter file to your binary directory to use it as a binary tool going forward. To import data from a CSV to a PostgreSQL table, execute the command below after modifying the required fields.

```
pgfutter --host <POSTGRES_HOSTNAME> --port <POSTGRES_PORT> --dbname <POSTGRES_DB> --username <POSTGRES_USERNAME> --pass <POSTGRES_PASSWORD> --schema <SCHEMA (such as public)> --table <TABLE_NAME (will be created if not already existing)> --ssl csv -d $'' --fields "<COMMA SEPARATED LIST OF ALL COLUMN HEADERS. THIS IS REQUIRED>" <CSV_FILE>.csv
```

Sample command:
```
pgfutter --host blah.blahblah.databases.appdomain.cloud --port 32557 --dbname ibmclouddb --username postgresuser --pass postgrespassword --schema public --table pgfuttertest --ssl csv -d $'^H' --fields "one,two,three,four,five" test_processed.csv
3.51 KiB / 3.51 KiB [==========================================================================================================================================================================================================] 100.00% 0s
10 rows imported into public.pgfuttertest
```
NOTE: The `-d` flag denotes the delimiter to use. In this case, the delimiter is a non-printable backspace character that looks like `BS` and the Unicode is '0x08'. The `$` is necessary to ensure that the shell doesn't interpret the delimiter and the `^H` that you see in the sample command is how the backspace symbol was interpreted in the CLI after I copy and pasted the full command from Sublime Text.

#### Runtime for inserting 10,000 rows with 43 columns: 1.389s

## Python (Python3)

The Python script will take a CSV file, parse it, and then upload the data to a PostgreSQL table. You can provide it with a CSV file locally or stored on AWS S3. The local file takes precedence. When uploading to a PostgreSQL table, the table will be created based on the values provided in the ini file if the table does not already exist.

To get started, create a new file called `import-database.ini` and copy the contents of `sample-import.ini` into it. Then, fill out the variables within for each section.

### Variables

#### PostgreSQL Section
```
Host: Hostname for your PostgreSQL instance
Port: Port for your PostgreSQL instance
Database: Name of the database in your PostgreSQL instance
User: Username with accessing to your PostgreSQL database
Password: Password corresponding to User
```

#### Table Section
```
Schema: Schema that your table exists/will be created in
Table: Table name
Columns: Comma separated list (no spaces) of columns to create. All columns created will be of type VARCHAR(255)
```
NOTE: The table will be created if not already existing. If the table does exist, it will not be replaced or overwritten nor will the columns be updated if the existing table columns differ from the columns provided in this section.

#### Local Section
```
Path: Path to the directory your CSV file is located
Filename: Filename of your CSV file, please include .csv file extension here
```

You can provide a `--delete` flag when running the Python script to have it delete the local file after importing the data to PostgreSQL.

NOTE: If the values here are populated, the local file will take precedence over AWS. Therefore, if you want to upload a file from AWS to PostgreSQL instead, please leave these values blank.

#### AWS Section
```
Access_Key_ID: AWS Access Key ID
Secret_Access_Key: AWS Secret Access Key
Bucket: Name of your bucket on AWS S3
Filename: Filename for your CSV in your S3 Bucket
```

#### IBM Section
```
API_Key_ID: API Key from your Cloud Object Storage (COS) instance credentials
Instance_ID: Instance ID of your COS instance
Auth_Endpoint: Authorization Endpoint for IBM Cloud. Typically this will be https://iam.cloud.ibm.com/identity/token
Endpoint: Endpoint for your COS instance based on reigon. This will look something like https://s3.ap.cloud-object-storage.appdomain.cloud
Bucket: Name of your Bucket on COS
Filename: Filename for your CSV in your COS bucket
```

### Running the Script
You need to install the required packages beforehand by either installing them manually or passing in the `requirements.txt` file to pip.
```
pip install -r requirements.txt
```

Run the python script. Depending on where you are pulling data from between Local, AWS, IBM, you must pass in a corresponding flag and have the ini section values populated. If you're providing a local file to import data from, you can pass in the `--delete` flag to have the file deleted after import.
```
python data-transfer.py import-database.ini --i --local/--aws/--ibm --delete
```

#### Runtime for inserting 10,000 rows with 43 columns: 3.313s

## psql
If you want to execute queries to insert data directly in Postgres using psql, you can do so after converting your CSV to a query. [CSV TO SQL](https://csvtosql.com/) is an easy to use online converter that supports the non-printable backspace character as a delimiter. Simply paste the contents of your CSV, paste the backspace character into the separator, and click Convert. You should then receive a SQL query for inserting the CSV data into the table (as well as a command to create a table which you shouldn't need). You may need to do a find-and-replace to insert the data into the correct table instead of the auto-generated one.

![CSV to SQL](csvtosql.png)

With your INSERT INTO query in hand, you can then execute it in the psql shell normally.



# Saving PostgreSQL tables to CSV and Uploading files to S3

## Export using Python (Python3)

The Python script when used with `Export` flag, will use the provided configuration file to connect to the database; loops through all the tables in a given database and schema. For each table, it exports the data to a CSV file created locally, uploads the file to AWS S3 and deletes the local file.

To get started, create a new file called `export-database.ini` and copy the contents of `sample-export.ini` into it. Then, fill out the variables within for each section.

### Variables

#### PostgreSQL Section
```
Host: Hostname for your PostgreSQL instance
Port: Port for your PostgreSQL instance
Database: Name of the database in your PostgreSQL instance
User: Username with accessing to your PostgreSQL database
Password: Password corresponding to User
```

#### Table Section
```
Schema: Schema that your tables exist in
```

#### Local Section
```
Path: Path to the directory to store the CSV files
```
NOTE: The name of the table is used as the CSV filename

#### AWS Section
```
Access_Key_ID: AWS Access Key ID
Secret_Access_Key: AWS Secret Access Key
Bucket: Name of your bucket on AWS S3
```
NOTE: Table name is used for the file uploaded

#### IBM Section
```
API_Key_ID: API Key from your Cloud Object Storage (COS) instance credentials
Instance_ID: Instance ID of your COS instance
Auth_Endpoint: Authorization Endpoint for IBM Cloud. Typically this will be https://iam.cloud.ibm.com/identity/token
Endpoint: Endpoint for your COS instance based on reigon. This will look something like https://s3.ap.cloud-object-storage.appdomain.cloud
```
NOTE: Table name is used for the file uploaded

### Running the Script
You need to install the required packages beforehand by either installing them manually or passing in the `requirements.txt` file to pip.
```
pip install -r requirements.txt
```

Run the python script. Depending on where you are exporting a CSV to between AWS S3 or IBM COS, you must pass in a corresponding flag and have the ini section values populated. You can optionally pass in the `--delete` flag to delete each local file created.
```
python data-transfer.py export-database.ini --e --aws/--ibm --delete
```

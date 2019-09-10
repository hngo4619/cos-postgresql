# Uploading a CSV file to PostgreSQL

## pgfutter (CLI)
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

To get started, create a new file called `database.ini` and copy the contents of `sample.ini` into it. Then, fill out the variables within for each section.

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
NOTE: If the values here are populated, the local file will take precedence over AWS. Therefore, if you want to upload a file from AWS to PostgreSQL instead, please leave these values blank.

#### AWS Section
```
Access_Key_ID: AWS Access Key ID
Secret_Access_Key: AWS Secret Access Key
Bucket: Name of your bucket on AWS S3
Filename: Filename for your CSV in your S3 Bucket
```

#### IBM Section (Work In Progress)
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

Run the python script.
```
python script.py
```

#### Runtime for inserting 10,000 rows with 43 columns: 3.313s

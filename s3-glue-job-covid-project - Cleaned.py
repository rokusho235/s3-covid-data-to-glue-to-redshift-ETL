import boto3
import pandas as pd
from io import StringIO #python3; python2: BytesI0
import time

############# CONNECTING TO ATHENA + QRY DATA #############

    AWS_ACCESS_KEY = "###"
    AWS_SECRET_KEY ="###"
    AWS_REGION = "us-east-1"
    SCHEMA_NAME = "covid-project" #whatever the DB name is in Athena
    S3_STAGING_DIR = "s3://.../.../"
    S3_BUCKET_NAME = "..."
    S3_OUTPUT_DIRECTORY = "..."

# connecting to athena

    athena_client = boto3.client(
        "athena",
        aws_access_key_id=AWS_ACCESS_KEY,
        aws_secret_access_key=AWS_SECRET_KEY,
        region_name=AWS_REGION,
    )

    Dict = {}
    def download_and_load_query_results(
        client: boto3.client, query_response: Dict
    ) -> pd.DataFrame:
        while True:
            try:
                # This function only loads the first 1000 rows
                client.get_query_results(
                    QueryExecutionId=query_response["QueryExecutionId"]
                )
                break
            except Exception as err:
                if "not yet finished" in str(err):
                    time.sleep(0.001)
                else:
                    raise err
        temp_file_location: str = "athena_query_results.csv"
        s3_client = boto3.client(
            "s3",
            aws_access_key_id=AWS_ACCESS_KEY,
            aws_secret_access_key=AWS_SECRET_KEY,
            region_name=AWS_REGION,
        )
        s3_client.download_file(
            S3_BUCKET_NAME,
            f"{S3_OUTPUT_DIRECTORY}/{query_response['QueryExecutionId']}.csv",
            temp_file_location,
        )
        return pd.read_csv(temp_file_location)

# pulling data from athena covid DB into csv and parking it in s3

    response = athena_client.start_query_execution(
        QueryString="SELECT * FROM enigma_jhud",
        QueryExecutionContext={"Database": SCHEMA_NAME},
        ResultConfiguration={
            "OutputLocation": S3_STAGING_DIR,
            "EncryptionConfiguration": {"EncryptionOption": "SSE_S3"},
        },
    )

# checking to see the "response"

# response
"""
{'QueryExecutionId':
...
  'RetryAttempts': 0}}
"""

# next going to pass the response dict into the "download_and_load_query_results" function
# storing the results as a dataframe obj

    enigma_jhud_df = download_and_load_query_results(athena_client, response)

# checking the dataframe = good

# enigma_jhud_df.head()

# now repeat for the rest of the tables--make sure to change the table name in the qry + df name

    response = athena_client.start_query_execution(
        QueryString="SELECT * FROM countrycode",
        QueryExecutionContext={"Database": SCHEMA_NAME},
        ResultConfiguration={
            "OutputLocation": S3_STAGING_DIR,
            "EncryptionConfiguration": {"EncryptionOption": "SSE_S3"},
        },
    )

    countrycode_df = download_and_load_query_results(athena_client, response)

    response = athena_client.start_query_execution(
        QueryString="SELECT * FROM countypopulation",
        QueryExecutionContext={"Database": SCHEMA_NAME},
        ResultConfiguration={
            "OutputLocation": S3_STAGING_DIR,
            "EncryptionConfiguration": {"EncryptionOption": "SSE_S3"},
        },
    )

    countypopulation_df = download_and_load_query_results(athena_client, response)

    response = athena_client.start_query_execution(
        QueryString="SELECT * FROM nytimes_data_in_usa_us_county",
        QueryExecutionContext={"Database": SCHEMA_NAME},
        ResultConfiguration={
            "OutputLocation": S3_STAGING_DIR,
            "EncryptionConfiguration": {"EncryptionOption": "SSE_S3"},
        },
    )

    nytimes_data_in_usa_us_county_df = download_and_load_query_results(athena_client, response)

    response = athena_client.start_query_execution(
        QueryString="SELECT * FROM nytimes_data_in_usa_us_states",
        QueryExecutionContext={"Database": SCHEMA_NAME},
        ResultConfiguration={
            "OutputLocation": S3_STAGING_DIR,
            "EncryptionConfiguration": {"EncryptionOption": "SSE_S3"},
        },
    )

    nytimes_data_in_usa_us_states_df = download_and_load_query_results(athena_client, response)

    response = athena_client.start_query_execution(
        QueryString="SELECT * FROM rearc_usa_hospital_beds",
        QueryExecutionContext={"Database": SCHEMA_NAME},
        ResultConfiguration={
            "OutputLocation": S3_STAGING_DIR,
            "EncryptionConfiguration": {"EncryptionOption": "SSE_S3"},
        },
    )

    rearc_usa_hospital_beds_df = download_and_load_query_results(athena_client, response)

    response = athena_client.start_query_execution(
        QueryString="SELECT * FROM state_abv",
        QueryExecutionContext={"Database": SCHEMA_NAME},
        ResultConfiguration={
            "OutputLocation": S3_STAGING_DIR,
            "EncryptionConfiguration": {"EncryptionOption": "SSE_S3"},
        },
    )

    state_abv_df = download_and_load_query_results(athena_client, response)

    response = athena_client.start_query_execution(
        QueryString="SELECT * FROM testing_data_states_daily",
        QueryExecutionContext={"Database": SCHEMA_NAME},
        ResultConfiguration={
            "OutputLocation": S3_STAGING_DIR,
            "EncryptionConfiguration": {"EncryptionOption": "SSE_S3"},
        },
    )

    testing_data_states_daily_df = download_and_load_query_results(athena_client, response)

    response = athena_client.start_query_execution(
        QueryString="SELECT * FROM testing_data_us_daily",
        QueryExecutionContext={"Database": SCHEMA_NAME},
        ResultConfiguration={
            "OutputLocation": S3_STAGING_DIR,
            "EncryptionConfiguration": {"EncryptionOption": "SSE_S3"},
        },
    )

    testing_data_us_daily_df = download_and_load_query_results(athena_client, response)

    response = athena_client.start_query_execution(
        QueryString="SELECT * FROM testing_data_us_total_latest",
        QueryExecutionContext={"Database": SCHEMA_NAME},
        ResultConfiguration={
            "OutputLocation": S3_STAGING_DIR,
            "EncryptionConfiguration": {"EncryptionOption": "SSE_S3"},
        },
    )

    testing_data_us_total_latest_df = download_and_load_query_results(athena_client, response)

# fixing the state_abv_df issue where col names are wrong + first row is "State, Abbreviation"
# data transformation step

    new_header = state_abv_df.iloc[0] # grab the first row/what the header is supposed to be
    state_abv_df = state_abv_df[1:] # slicing/getting all the data besides the first row
    state_abv_df.columns = new_header # assigning/setting the columns with the right names

############# ETL JOB IN PYTHON #############
# converting relational DM to dimension model

    factCovid_1 = enigma_jhud_df[['fips','province_state', 'country_region', 'confirmed', 'deaths','recovered', 'active']]
    factCovid_2 = testing_data_states_daily_df[['fips', 'date', 'positive', 'negative', 'hospitalizedcurrently', 'hospitalized', 'hospitalizeddischarged']]
    factCovid = pd.merge(factCovid_1, factCovid_2, on='fips', how='inner')

# testing the fact table

"""
factCovid.head()
factCovid.shape # tells the rows + cols in the table
"""

# doing the dimRegion table

    dimRegion_1 = enigma_jhud_df[['fips','province_state','country_region','latitude','longitude']]
    dimRegion_2 = nytimes_data_in_usa_us_county_df[['fips','county', 'state']]
    dimRegion = pd.merge(dimRegion_1, dimRegion_2, on='fips', how='inner')

    dimHospital = rearc_usa_hospital_beds_df[['fips','state_name','latitude','longtitude','hq_address', 'hospital_name', 'hospital_type','hq_city', 'hq_state']]

    dimDate = testing_data_states_daily_df[['fips','date']]

# converting YYYYMMDD int to the right format

    dimDate['date'] = pd.to_datetime(dimDate['date'], format='%Y%m%d')

    dimDate.head()

# customized columns: "month", "year", "is_weekend" have to be manually created

    dimDate['year'] = dimDate['date'].dt.year
    dimDate['month'] = dimDate['date'].dt.month
    dimDate["day_of_week"]= dimDate['date'].dt.dayofweek

    # dimDate.head()
    # now we have the extra cols

############# SAVING RESULTS OF FACT/DIMENSION TABLES TO S3 #############

    bucket = 'covid-to-redshift'

# "how to store data into s3 with python"

    s3_resource = boto3.resource(
        's3',
        region_name=AWS_REGION,
        aws_access_key_id=AWS_ACCESS_KEY,
        aws_secret_access_key=AWS_SECRET_KEY
    )

    try:
        from StringIO import StringIO
    except ImportError:
        from io import StringIO

# decided to create separate csv_buffer objects because i was getting a bug where the buffer seemed to just concat instead of clearing between runs

    factCovid_csv_buffer = StringIO()

#factCovid_csv_buffer = StringI0() # used to put values into binary format/buffer so then we can write it to s3
    factCovid.to_csv(factCovid_csv_buffer) # writing fact tbl into buffer
    s3_resource.Object(bucket, 'covidStaging/factCovid.csv').put(Body=factCovid_csv_buffer.getvalue()) # csv_buffer.getvalue() spits out csv format of the factCovid table

    dimHospital_csv_buffer = StringIO()

    dimHospital.to_csv(dimHospital_csv_buffer) # writing dim tbl into buffer
    s3_resource.Object(bucket, 'covidStaging/dimHospital.csv').put(Body=dimHospital_csv_buffer.getvalue()) # csv_buffer.getvalue() spits out csv format of the tbl

    dimDate_csv_buffer = StringIO()

    dimDate.to_csv(dimDate_csv_buffer) # writing dim tbl into buffer
    s3_resource.Object(bucket, 'covidStaging/dimDate.csv').put(Body=dimDate_csv_buffer.getvalue()) # csv_buffer.getvalue() spits out csv format of the tbl

    dimRegion_csv_buffer = StringIO()

    dimRegion.to_csv(dimRegion_csv_buffer) # writing dim tbl into buffer
    s3_resource.Object(bucket, 'covidStaging/dimRegion.csv').put(Body=dimRegion_csv_buffer.getvalue()) # csv_buffer.getvalue() spits out csv format of the tbl

# the above process/tbl will take a really long time cuz it's like 2 GB

# extracting schema out of the pandas dataframes--basically automates the SQL DDL commands for tbl creation
# choosing to store the SQL into a str var to use during the redshift connection part

    dimDatesql = pd.io.sql.get_schema(dimDate.reset_index(), 'dimDate')
    dimDatesql = (''.join(dimDatesql))

    """
    CREATE TABLE "dimDate" (
    "index" INTEGER,
      "fips" REAL,
      "date" TIMESTAMP,
      "year" INTEGER,
      "month" INTEGER,
      "day_of_week" INTEGER
    )
    """

    factCovidsql = pd.io.sql.get_schema(factCovid.reset_index(), 'factCovid')
    factCovidsql = (''.join(factCovidsql))

    dimRegionsql = pd.io.sql.get_schema(dimRegion.reset_index(), 'dimRegion')
    dimRegionsql = (''.join(dimRegionsql))

    dimHospitalsql = pd.io.sql.get_schema(dimHospital.reset_index(), 'dimHospital')
    dimHospitalsql = (''.join(dimHospitalsql))

# using redshift connector library instead of psycopg2 to connect to redshift

    import redshift_connector as rs_conn

    conn = rs_conn.connect(
            host='redshift-cluster-1.###.us-east-1.redshift.amazonaws.com',
            database='dev',
            user="awsuser",
            password='###'
        )

    conn.autocommit = True

# creating cursor that executes the SQL statements

    cursor = rs_conn.Cursor = conn.cursor()

# creating the tables in redshift

    cursor.execute(factCovidsql)
    cursor.execute(dimDatesql)
    cursor.execute(dimHospitalsql)
    cursor.execute(dimRegionsql)

# copying data from s3 to redshift

    str1 = "copy "
    str2 = " from 's3://.../.../"
    str3 = ".csv'\ncredentials 'aws_iam_role=arn:aws:iam::###:role/###'\ndelimiter ','\nregion 'us-east-1'\nIGNOREHEADER 1"
    tblName = "dimDate"

    copySql = str1+tblName+str2+tblName+str3

    cursor.execute(copySql)

    tblName = "dimHospital"
    copySql2 = str1+tblName+str2+tblName+str3
    cursor.execute(copySql2)

    tblName = "dimRegion"
    copySql3 = str1+tblName+str2+tblName+str3
    cursor.execute(copySql3)

    tblName = "factCovid"
    copySql4 = str1+tblName+str2+tblName+str3
    cursor.execute(copySql4)
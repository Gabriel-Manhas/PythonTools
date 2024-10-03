import io
import PyPDF2
import filetype
import pymssql
import boto3

#SQL tools
def connect(server, username, password, database):
    try:
        conn = pymssql.connect(server=server,
                               user=username,
                               password=password,
                               database=database,
                               tds_version='7.0',
                               timeout=1)
        return {'Fail': False, 'Result': conn}
    
    except pymssql.Error as e:
        return {'Fail': True, 'Error': f"Database connection failed: {e}"}

def get_all_rows_field_from_table(conn, field, table):
    cursor = conn.cursor()
    
    try:
        query = f"SELECT {field} FROM {table}"
        cursor.execute(query)
        rows = cursor.fetchall()
        result = [row[0] for row in rows]
        return {'Fail': False, 'Result': result}

    except pymssql.DatabaseError as e:
        return {'Fail': True, 'Error': f"Database error: {e}"}

def get_primary_key_column(conn, table_name):
    query = f"""
    SELECT column_name 
    FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE 
    WHERE OBJECTPROPERTY(OBJECT_ID(constraint_name), 'IsPrimaryKey') = 1 
    AND table_name = '{table_name}'
    """

    if conn:
        try:
            with conn.cursor(as_dict=True) as cursor:
                cursor.execute(query)
                result = cursor.fetchone()
                if result:
                    return {'Fail': False, 'Result': result['column_name']}
                else:
                    return {'Fail': True, 'Error': 'Primary key not found'}
        except pymssql.Error as e:
            return {'Fail': True, 'Error': str(e)}
    return {'Fail': True, 'Error': 'Connection not established'}

def insert_data(conn, table_name, data):
    cleaned_data = drop_none_data(data)
    columns = ', '.join([item[0] for item in cleaned_data])
    query = f"INSERT INTO {table_name} ({columns}) VALUES ({', '.join(['%s'] * len(cleaned_data))})"
    values = tuple(item[1] if item[1] is not None else None for item in cleaned_data)

    if conn:
        try:
            with conn.cursor() as cursor:
                cursor.execute(query, values)
                conn.commit()
                return {'Fail': False, 'Result': 'Insert successful'}
        except pymssql.Error as e:
            return {'Fail': True, 'Error': str(e)}
    return {'Fail': True, 'Error': 'Connection failed'}

def insert_and_get_pk(conn, table_name, data):
    pk_column = get_primary_key_column(conn, table_name)
    if pk_column['Fail']:
        return {'Fail': True, 'Error': 'Primary key not found'}
    else:
        pk_column = pk_column['Result']

    cleaned_data = drop_none_data(data)
    columns = ', '.join([item[0] for item in cleaned_data])
    query = f"INSERT INTO {table_name} ({columns}) OUTPUT INSERTED.{pk_column} VALUES ({', '.join(['%s'] * len(cleaned_data))})"
    values = tuple(item[1] if item[1] is not None else None for item in cleaned_data)

    if conn:
        try:
            with conn.cursor(as_dict=True) as cursor:
                cursor.execute(query, values)
                pk = cursor.fetchone()[pk_column]
                conn.commit()
                return {'Fail': False, 'Result': pk}
        except pymssql.Error as e:
            return {'Fail': True, 'Error': str(e)}
    return {'Fail': True, 'Error': 'Connection not established'}

def get_first_match_in_table(conn, table_name, column_name, column_value):
    pk_column = get_primary_key_column(conn, table_name)
    if pk_column['Fail']:
        return {'Fail': True, 'Error': 'Primary key not found'}
    
    pk_column_name = pk_column['Result']
    query = f"SELECT {pk_column_name} FROM {table_name} WHERE {column_name} like %s"
    
    if conn:
        try:
            with conn.cursor(as_dict=True) as cursor:
                cursor.execute(query, (column_value,))
                result = cursor.fetchone()
                if result:
                    return {'Fail': False, 'Result': result[pk_column_name]}
                else:
                    return {'Fail': True, 'Result': None}
        except pymssql.Error as e:
            return {'Fail': True, 'Error': str(e)}
    return {'Fail': True, 'Error': 'Connection not established'}

def get_all_matches_in_table(conn, table_name, column_name, column_value):
    pk_column = get_primary_key_column(conn, table_name)
    if pk_column['Fail']:
        return {'Fail': True, 'Error': 'Primary key not found'}
    
    pk_column_name = pk_column['Result']
    query = f"SELECT {pk_column_name} FROM {table_name} WHERE {column_name} = %s"
    
    if conn:
        try:
            with conn.cursor(as_dict=True) as cursor:
                cursor.execute(query, (column_value,))
                results = cursor.fetchall()
                if results:
                    return {'Fail': False, 'Result': [row[pk_column_name] for row in results]}
                else:
                    return {'Fail': False, 'Result': None}
        except pymssql.Error as e:
            return {'Fail': True, 'Error': str(e)}
    return {'Fail': True, 'Error': 'Connection not established'}

def insert_or_get(conn, table_name, column_name, value):
    existing_pk = get_first_match_in_table(conn, table_name, column_name, value)
    if existing_pk['Fail']:
        return existing_pk  # If there was an error checking the value, return the error

    if existing_pk['Result'] is not None:
        return {'Fail': False, 'Result': existing_pk['Result']}
    else:
        inserted_pk = insert_and_get_pk(conn, table_name, [(column_name, value)])
        return inserted_pk  # Return the result from insert_and_get_pk

def get_values_by_pk(conn, table_name, pk_value, columns):
    """
    Retrieves specific column values from a row in the table, identified by the primary key.
    
    :param conn: The database connection.
    :param table_name: The name of the table.
    :param pk_value: The primary key value.
    :param columns: A list of column names whose values you want to retrieve.
    :return: A dictionary indicating success or failure, and the result if successful.
    """
    # Convert the list of columns to a comma-separated string
    columns_str = ', '.join(columns)

    pk_column = get_primary_key_column(conn, table_name)
    if pk_column['Fail']:
        return {'Fail': True, 'Error': 'Primary key not found'}
    
    pk_column_name = pk_column['Result']

    query = f"SELECT {columns_str} FROM {table_name} WHERE {pk_column_name} = %s"

    if conn:
        try:
            with conn.cursor(as_dict=True) as cursor:
                cursor.execute(query, (pk_value,))
                result = cursor.fetchone()
                if result:
                    return {'Fail': False, 'Result': result}
                else:
                    return {'Fail': True, 'Error': "No row found for the given PK"}  # No row found for the given PK
        except pymssql.Error as e:
            return {'Fail': True, 'Error': str(e)}
    return {'Fail': True, 'Error': 'Connection not established'}

def update_data(conn, table_name, data, pk_value):
    """
    Update specific columns in a row identified by the primary key.
    
    :param conn: The database connection.
    :param table_name: The name of the table.
    :param data: A list of tuples, each containing (column_name, column_value) to be updated.
    :param pk_value: The primary key value identifying the row to update.
    :return: A dictionary indicating success or failure.
    """
    pk_column = get_primary_key_column(conn, table_name)
    if pk_column['Fail']:
        return {'Fail': True, 'Error': 'Primary key not found'}
    
    pk_column_name = pk_column['Result']
    cleaned_data = drop_none_data(data)
    set_clause = ', '.join([f"{item[0]} = %s" for item in cleaned_data])
    values = tuple(item[1] if item[1] is not None else None for item in cleaned_data) + (pk_value,)

    query = f"UPDATE {table_name} SET {set_clause} WHERE {pk_column_name} = %s"

    if conn:
        try:
            with conn.cursor() as cursor:
                cursor.execute(query, values)
                conn.commit()
                return {'Fail': False, 'Result': 'Update successful'}
        except pymssql.Error as e:
                return {'Fail': True, 'Error': str(e)}
    return {'Fail': True, 'Error': 'Connection not established'}

def call_stored_procedure(conn, procedure_name, params):
    """
    Calls a stored procedure in the SQL server.
    
    :param conn: The database connection.
    :param procedure_name: The name of the stored procedure to call.
    :param params: A tuple of parameters to pass to the stored procedure.
    :return: A dictionary indicating success or failure, and the result if successful.
    """
    if conn:
        try:
            with conn.cursor(as_dict=True) as cursor:
                # Build the EXEC command with named parameters
                param_placeholders = ', '.join([f"@{key}=%s" for key in params.keys()])
                exec_command = f"EXEC {procedure_name} {param_placeholders}"

                # Execute the stored procedure
                cursor.execute(exec_command, tuple(params.values()))

                # Fetch the results
                results = cursor.fetchall()

                conn.commit()
                return {'Fail': False, 'Result': results}
        except pymssql.Error as e:
            return {'Fail': True, 'Error': str(e)}
    return {'Fail': True, 'Error': 'Connection not established'}

# S3 tools
def S3_connect(S3_ACCESS_KEY_ID, S3_ACCESS_KEY, S3_ENDPOINT_URL):
    try:
        s3_client = boto3.client('s3', aws_access_key_id=S3_ACCESS_KEY_ID, aws_secret_access_key=S3_ACCESS_KEY, endpoint_url=S3_ENDPOINT_URL)
        return {'Fail': False, 'Result': s3_client}
    
    except Exception as e:
            return {'Fail': True, 'Error': f"S3 connection failed: {e}"}

def upload_to_s3(s3_client, file_content, bucket, key, content_type = 'application/octet-stream'):
    """
    Helper function to upload a file to S3.
    :param file_content: The file content to upload.
    :param bucket: The S3 bucket name.
    :param key: The key (path) where the file will be uploaded.
    """
    try:
        response = s3_client.put_object(
            Bucket=bucket,
            Key=key,
            Body=file_content,
            ContentType=content_type  # Adjust the content type if necessary
        )
        
        # Check the HTTP status code
        if response['ResponseMetadata']['HTTPStatusCode'] == 200:
            return True
        else:
            return False
    except Exception as e:
        return False

# Documents processing tools
def get_pdf_pages(file_content):
    try:
        # Read the PDF file content from binary data
        reader = PyPDF2.PdfReader(io.BytesIO(file_content))
        # Get the total number of pages
        return len(reader.pages)
    except Exception as e:
        raise Exception(f"An error occurred while extracting the number of pages: {str(e)}")

def get_content_type(file_content):
    try:
        kind = filetype.guess(file_content)
        if kind is None:
            raise Exception("Cannot guess file type.")
        return kind.mime
    except Exception as e:
        raise Exception(f"An error occurred while detecting content type: {str(e)}")

# Data processing tools
def drop_none_data(data):
    """
    Remove tuples with None values from the data list.
    :param data: List of tuples, each containing (column_name, column_value).
    :return: Cleaned list of tuples without any None values.
    """
    return [(col, val) for col, val in data if val is not None]

#Datadog Logging Tools

import logging
from datadog import initialize, api
from datadog import statsd

class DatadogLogger:
    def __init__(self, api_key, app_key, env='prod', script_name='default_script'):
        # Initialize Datadog
        self.options = {
            'api_key': api_key,
            'app_key': app_key,
        }
        initialize(**self.options)

        # Set up logger
        self.logger = logging.getLogger(script_name)
        self.logger.setLevel(logging.INFO)

        # Add DatadogHandler
        datadog_handler = self.DatadogHandler(env, script_name)
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        datadog_handler.setFormatter(formatter)
        self.logger.addHandler(datadog_handler)

    class DatadogHandler(logging.Handler):
        def __init__(self, env, script_name):
            super().__init__()
            self.env = env
            self.script_name = script_name

        def emit(self, record):
            log_entry = self.format(record)
            api.Event.create(
                title="Python Log",
                text=log_entry,
                tags=[f"env:{self.env}", f"script:{self.script_name}"]
            )

    def log_info(self, message):
        self.logger.info(message)

    def log_error(self, message):
        self.logger.error(message)

    def log_metric(self, metric_name, value=1):
        statsd.increment(metric_name, value)
from google.cloud import bigquery
from google.cloud.exceptions import NotFound
from google.oauth2 import service_account
from config import config
import pandas as pd
import psycopg2
import os

# connect to database and create dataframe
def extract_from_pg():
    conn = None
    try:
        params = config()
        conn = psycopg2.connect(**params)
        cur = conn.cursor()
        cur.execute('''SELECT * FROM course_enrollment''')
        query = cur.fetchall()

        print('Creating dataframe...')
        cols = []
        for i in cur.description:
            cols.append(i[0])
        
        df = pd.DataFrame(data=query, columns=cols)
        cur.close()

    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()
    
    return df

# create dataset in BigQuery
def create_dataset_bq(client, dataset):
    dataset_ref = client.dataset(dataset)

    try:
        dataset = client.get_dataset(dataset_ref)
        print('Dataset {} already exists.'.format(dataset_ref.dataset_id))

    except NotFound:
        dataset = bigquery.Dataset(dataset_ref)
        dataset.location = 'asia-southeast1'
        dataset = client.create_dataset(dataset)
        print('Successfully created dataset {}.'.format(dataset.dataset_id))

    return dataset

# load to BigQuery
def load_to_bq(df, dataset):
    credentials = service_account.Credentials.from_service_account_file('capstone-project-team-d-410112-38d07292b34b.json')
  
    #define new table name that will be created based on the Pandas DataFrame
    table_id = 'fact_course_enrollment'
    schema = [
        {'name': 'student_id', 'type': 'STRING', 'mode': 'REQUIRED'},
        {'name': 'course_id', 'type': 'STRING', 'mode': 'REQUIRED'}
    ]

    try:
        df.to_gbq(destination_table=f'{dataset}.{table_id}', if_exists='replace', table_schema=schema, credentials=credentials)
        print('Successfully created table {}.'.format(table_id))

    except Exception as e:
        print(f'An unexpected error occurred: {e}')


if __name__ == '__main__':
    project_id = os.getenv('PROJECT_ID')
    df = extract_from_pg()
    client = bigquery.Client(project=project_id)
    dataset = 'dwh'
    create_dataset_bq(client, dataset)
    load_to_bq(df, dataset)


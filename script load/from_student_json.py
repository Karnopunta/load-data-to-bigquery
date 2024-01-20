from google.cloud import bigquery
from google.cloud.exceptions import NotFound
from google.oauth2 import service_account
import pandas as pd
import os


# extract from csv
def extract():
    df = pd.read_json('../data-source/student.json')

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
    table_id = 'dim_student'
    schema = [
        {'name': 'id', 'type': 'STRING', 'mode': 'REQUIRED'},
        {'name': 'first_name', 'type': 'STRING'},
        {'name': 'last_name', 'type': 'STRING'},
        {'name': 'street_address', 'type': 'STRING'},
        {'name': 'country', 'type': 'STRING'},
        {'name': 'city', 'type': 'STRING'},
        {'name': 'phone_number', 'type': 'STRING'}
    ]

    try:
        df.to_gbq(destination_table=f'{dataset}.{table_id}', if_exists='replace', table_schema=schema, credentials=credentials)
        print('Successfully created table {}.'.format(table_id))

    except Exception as e:
        print(f'An unexpected error occurred: {e}')


if __name__ == '__main__':
    project_id = os.getenv('PROJECT_ID')
    df = extract()
    client = bigquery.Client(project=project_id)
    dataset = 'dwh'
    create_dataset_bq(client, dataset)
    load_to_bq(df, dataset)


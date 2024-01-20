from google.cloud import bigquery
from google.cloud.exceptions import NotFound
from google.oauth2 import service_account
import pandas as pd
import os

# run query from BigQuery
def run_query(project_id):
    query = '''
        select
            course_category_id
            , type as course_type
            , course_id
            , title as course_title
            , student_id
            , concat(s.first_name, ' ', s.last_name) as student_name
            , city
            , mentor_id
            , concat(m.first_name, ' ', m.last_name) as mentor_name
        from `dwh.dim_student` as s
        join `dwh.fact_course_enrollment` as ce 
            on ce.student_id = s.id
        join `dwh.fact_course` as c
            on c.id = ce.course_id
        join `dwh.dim_course_category` as cc
            on cc.id = c.course_category_id 
        join `dwh.dim_mentor` m 
            on c.mentor_id = m.id;
    '''

    df = pd.read_gbq(query, project_id=project_id)
    
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

# define schema and create new table for datamart
def create_datamart_table(df, dataset):
    credentials = service_account.Credentials.from_service_account_file('capstone-project-team-d-410112-38d07292b34b.json')
    
    table_id = 'course_enrollment_summary'
    schema = [
        {'name': 'course_category_id', 'type': 'STRING'},
        {'name': 'course_type', 'type': 'STRING'},
        {'name': 'course_id', 'type': 'STRING'},
        {'name': 'course_title', 'type': 'STRING'},
        {'name': 'student_id', 'type': 'STRING'},
        {'name': 'student_name', 'type': 'STRING'},
        {'name': 'city', 'type': 'STRING'},
        {'name': 'mentor_id', 'type': 'STRING'},
        {'name': 'mentor_name', 'type': 'STRING'}
    ]

    try:
        df.to_gbq(destination_table=f'{dataset}.{table_id}', if_exists='replace', table_schema=schema, credentials=credentials)
        print('Successfully created table {}.'.format(table_id))

    except Exception as e:
        print(f'An unexpected error occurred: {e}')


if __name__ == '__main__':
    project_id = os.getenv('PROJECT_ID')
    df = run_query(project_id)
    client = bigquery.Client(project=project_id)
    dataset = 'datamart'
    create_dataset_bq(client, dataset)
    create_datamart_table(df, dataset)

import psycopg2
import pandas as pd
from config import config

def connect():
    conn = None
    try:
        # read connection parameters
        params = config()

        # connect to the PostgreSQL server
        print('Connecting to the PostgreSQL database...')
        conn = psycopg2.connect(**params)
		
        # create a cursor
        cur = conn.cursor()
        
	# execute a statement
    
        # print('PostgreSQL table list:')
        # cur.execute(
        #     '''
        #     SELECT
        #         table_name
        #     FROM
        #         information_schema.tables
        #     WHERE
        #         table_schema = 'public'
        #     AND table_type = 'BASE TABLE';
        #     '''
        # )
        # query = cur.fetchone()
        # print(query)

        print('extract from course_enrollment table:')
        cur.execute(
            '''
            SELECT
                *
            FROM
                course_enrollment
            '''
        )
        
        query = cur.fetchall()
        
        # create dataframe
        cols = []
        for i in cur.description:
            cols.append(i[0])
        
        df = pd.DataFrame(data=query, columns=cols)
        print(df)
        
	# close the communication with the PostgreSQL
        cur.close()
        
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
        
    finally:
        if conn is not None:
            conn.close()
            print('Database connection closed.')


if __name__ == '__main__':
    connect()


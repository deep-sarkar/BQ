from google.cloud import bigquery
from google.oauth2 import service_account
import pandas_gbq


#variable
gcp_project = 'bigquerypratice'
key_path    = "C:/Users/Dell/Desktop/python/BQ/bigquerypratice-6746c12b0f6f.json"

#connections
credentials = service_account.Credentials.from_service_account_file(
    key_path, scopes=["https://www.googleapis.com/auth/cloud-platform"],)

pandas_gbq.context.credentials = credentials
pandas_gbq.context.project = gcp_project

def query_to_BQ(query):
    client = bigquery.Client(credentials=credentials, project=gcp_project)
    query = client.query(query=query)
    query_result = query.result()
    return query_result.to_dataframe()

def load_df_to_BQ(df, table_id):
    df.to_gbq(table_id)
    return 1


if __name__ == "__main__":
    query = '''
            SELECT * 
            FROM `bigquerypratice.joins.actual_spend`
            WHERE Clicks > 4000 and Clicks < 8000
            '''
    df = query_to_BQ(query)
    table = "from_panda_df.table_from_pandas"
    load = load_df_to_BQ(df, table)
    

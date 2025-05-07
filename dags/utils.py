import awswrangler as wr
import boto3
import pandas as pd
import requests
from airflow.models import Variable

session = boto3.session.Session(
                aws_access_key_id=Variable.get("AWS_ACCESS_KEY_ID"),
                aws_secret_access_key=Variable.get("AWS_SECRET_ACCESS_KEY"),
                region_name=Variable.get("REGION_NAME"))


def get_response():
    """
    To fetch the data from the randomuser API.
    """
    URL = 'https://randomuser.me/api/?results=5'
    response = requests.get(URL)
    return response.json()["results"]


def convert_all_data(all_data):
    """
    Convert the extracted data to Dataframe, string datatype and normalize it.
    """
    all_data_new = pd.json_normalize(all_data)
    all_data_new = all_data_new.astype(str)
    return all_data_new


def load_df_to_s3(df):
    """"
    Load the dataframe to s3 bucket.
    """
    s3_pathway = "s3://victorakinbami/API_random_folder1/new_task_file.parquet"
    wr.s3.to_parquet(
                 df=df,
                 path=s3_pathway,
                 index=False,
                 boto3_session=session,
                 dataset=True,
                 mode="append"
                )


print("successfully uploaded")


def full_pipeline():
    """
    Extract, transform and load.
    """
    extract = get_response()
    transform = convert_all_data(extract)
    load_df_to_s3(transform)
    return "Successfull"

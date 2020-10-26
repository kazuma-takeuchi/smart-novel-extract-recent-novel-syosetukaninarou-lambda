import boto3

def build_client_dynamo(table_name="control"):
    dynamodb = boto3.resource('dynamodb')
    return dynamodb.Table(table_name)
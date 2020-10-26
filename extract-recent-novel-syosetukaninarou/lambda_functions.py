import os
import re
import json
import time
import requests
import logging

from datetime import datetime
from bs4 import BeautifulSoup
from boto3.dynamodb.conditions import Key


from connections import build_client_dynamo


PKEY = os.getenv("PKEY")
SKEY = os.getenv("SKEY")
TABLE_NAME = os.getenv("TABLE_NAME")


def get_info(pkey, skey):
    table = build_client_dynamo(table_name=TABLE_NAME)
    info = table.query(
        KeyConditionExpression = Key("pkey").eq(pkey) & Key("skey").eq(skey)
    )
    return info['Items'][0]
    

def extract_recent_link(url):
    params = {
        "order": "new",
        "of": "n",
        "lim": 100,
        "out": "json"
    }
    res = requests.get(url, params=params)
    links = [l["ncode"].lower() for l in json.loads(res.text)[1:]]
    return links
    
    
def remove_duplicated_link(links, lastkey, default=False):
    idx = links.index(lastkey) if lastkey in links else default
    return links[:idx]
    
    
def lambda_handler(event, context):
    info = get_info(PKEY, SKEY)
    url = info['url']
    lastkey = info['lastkey']
    links = extract_recent_link(url)
    links = remove_duplicated_link(links, lastkey, len(links))
    if len(links) > 0:
        lastkey = links[0]
    return {
        'statusCode': 200,
        'pkey': info['pkey'],
        'skey': info['skey'],
        'lastkey': lastkey,
        'links': links
    }

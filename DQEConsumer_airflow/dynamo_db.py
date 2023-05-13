#######################################
#### Jean Wu 20211002
#### dynamoDB operation
######################################

from pprint import pprint
import boto3
import json
from decimal import Decimal
from datetime import datetime, timezone as tz
import numpy as np, math
import os, gzip, simplejson
import zlib
from boto3.dynamodb.conditions import Key
from boto3.dynamodb.types import Binary
import pprint
#boto3.setup_default_session(profile_name='default')
#boto3.client('sts').get_caller_identity()
#print("AWS get_caller_identity:", awsid)

## time
def what_time_is_it():

    """This function gets the time 'now' in UTC format
    The output is formatted as yyyy-mm-ddTHH:MM:SS.fffZ 
    where fff is 3 decimal point precision microseconds and Z is UTC timezone.
    This format is the same as the geoserver"""

    # datetime object containing current date and time
    now = datetime.now(tz.utc)
    nowms = str(np.round(now.microsecond*1e-3).astype(np.int))
    frmt = "%Y-%m-%dT%H:%M:%S"
    # dd/mm/YY H:M:S
    dt_string = now.strftime(frmt)
    dt_string = dt_string+"."+nowms+"Z"
    return dt_string

'''
Put one items 
Impose error handeling for limitation of 400KB 
- use decoding/encoding to binary
- pop out the item
'''
def put_one_dqe_item(tpt_id_key, year, dqe_results, timestamp, doc_type, table_name, dynamodb=None):
    if not dynamodb:
        dynamodb = boto3.resource('dynamodb')

    table = dynamodb.Table(table_name)
    data_json = {
            'tpt_id_key': tpt_id_key,
            'trial_year': year,
            'timestamp': timestamp,
            'dqe_results': dqe_results,
            'doc_type': doc_type
    }
    transform_json = json.loads(json.dumps(data_json), parse_float=Decimal)
    error_flag = 0
    try:
        response = table.put_item( Item= transform_json)
        error_flag = 0
    except Exception as e1:
        print('******** error when push results to dynamodb *******')
        print([tpt_id_key, str(e1)])

        input_compressed = {}
        if 'TrialCompleteness' in dqe_results:
            input_compressed = dqe_results['TrialCompleteness']['input']
            dqe_results['TrialCompleteness']['input'] = {}
        elif 'TDCompleteness_0' in dqe_results:
            input_compressed = dqe_results['TDCompleteness_0']['input']
            dqe_results['TDCompleteness_0']['input'] = {}
        elif 'TDCompleteness_1' in dqe_results:
            input_compressed = dqe_results['TDCompleteness_1']['input']
            dqe_results['TDCompleteness_1']['input'] = {}
        input_compressed_str = simplejson.dumps(input_compressed)
        input_byte = input_compressed_str.encode('utf-8')
        compressed = zlib.compress(input_byte)
        transform_json = json.loads(json.dumps(dqe_results), parse_float=Decimal)

        response = table.put_item( 
            Item = {
                'tpt_id_key': tpt_id_key,
                'trial_year': year,
                'timestamp': timestamp,
                'dqe_results': transform_json,
                'doc_type': doc_type,
                'compressed_input': compressed
                }
            )
        error_flag = 1
    return error_flag, response


def query_tpt_id_key(tpt_id_key, limt_num, table_name, dynamodb=None):
    if not dynamodb:
        dynamodb = boto3.resource('dynamodb')

    table = dynamodb.Table(table_name)
    ## query tpt_id_key, order by trial_year, descending
    response = table.query(
        ScanIndexForward=False, Limit=limt_num,
        KeyConditionExpression=
            Key('tpt_id_key').eq(tpt_id_key)
            
    )
    return response['Items']


# ## only for testing --query id, decompress
# if __name__ == '__main__':
#     table_name = 'dqe_results_dev' 
#     tpt_id_key = "ID22BRAO23IS01"
#     limt_num = 1
#     return_items = query_tpt_id_key(tpt_id_key, limt_num, table_name)
#     print('len of results:', len(return_items))
#     for x in return_items:
#         print('========================')
#         print([x['tpt_id_key'], x['trial_year'], x['timestamp']])
#         print(x.keys())
#         print(type(x['compressed_input'])) 
#         print(type(x['compressed_input'].value)) 
#         print(zlib.decompress(x['compressed_input'].value))
#         teststr = zlib.decompress(x['compressed_input'].value)
#         temp_dict = json.loads(teststr)
#         print(type(temp_dict))
#         pprint.pprint(temp_dict, indent=1)

## only for testing - put an item
# if __name__ == '__main__':
#     table_name = 'dqe_results_dev'
#     tpt_id_key = "ID21ARGM1JWR"
#     year = 2021
#     # curr_timestamp = datetime.datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%S')
#     curr_timestamp = what_time_is_it()    
#     dqe_results = {'score': 105.5, 'missing_fields': 'dummytest'}
#     doc_type = 'TD'
#     # dqe_score = dqe_results['score'] # score
#     response = put_one_dqe_item(tpt_id_key, year, dqe_results, curr_timestamp, doc_type, table_name)
#     print("Put one item succeeded:")
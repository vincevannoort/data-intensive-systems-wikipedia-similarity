import sys
import glob
import itertools
import json
import statistics
import asyncio
import boto3
from typing import Tuple, List
from config import settings
from concurrent.futures import ThreadPoolExecutor
from cloud_functions.compare_two_pages import compare_two_files
from config import settings

def get_files_list() -> List[str]:
    return(glob.glob(f'./{settings.data_folder}/*.json'))

def make_permutations(files: List[str]):
    return list(itertools.combinations(files, 2))

def open_files_and_compare(files: Tuple[str, str]):
    (file1, file2) = files
    file_1_id = file1.split('-')[0].split('/')[-1]
    file_2_id = file2.split('-')[0].split('/')[-1]
    with open(file1, 'r') as file1_data, open(file2, 'r') as file2_data:
        file_1_json = json.load(file1_data)
        file_2_json = json.load(file2_data)
        score = compare_two_files(file_1_id, file_2_id, file_1_json, file_2_json)
        return score

def store_result_in_dynamo(result):
    # key, score = result

    # session = boto3.Session(
    #     aws_access_key_id = settings['aws_access_key_id'],
    #     aws_secret_access_key = settings['aws_secret_access_key'],
    #     aws_session_token = settings['aws_session_token']
    # )

    # dynamodb_client = session.resource('dynamodb', 'us-east-1')

    
    # table = dynamodb_client.Table('data-intensive-database')
    # response = table.put_item(
    #    Item={
    #         'relationCombinationId': key,
    #         'score': score,
    #     }
    # )

    # print(f'stored result: {key} -> {score}')
    
    return True

def compare_files_local(spark, session):
    files = get_files_list()
    perms = make_permutations(files)
    parallel_perms = spark.parallelize(perms)
    parallel_perms_map_result = parallel_perms.map(open_files_and_compare)
    parallel_perms_map_result_stored = parallel_perms_map_result.map(lambda result: store_result_in_dynamo(result))
    print(parallel_perms_map_result_stored.collect())

    print(f'[PERM AMOUNT]: {len(perms)}')

async def compare_files_cloud(spark, session):
    s3_client = session.client('s3', 'us-east-1')
    bucket = s3_client.list_objects(Bucket='data-intensive-storage')['Contents']
    files = [file['Key'] for file in bucket]
    perms = make_permutations(files)

    print(f'[PERM AMOUNT]: {len(perms)}')

    def compare_files(perm):
        print(f'starting: {perm}')
        lambda_client = session.client('lambda', 'us-east-1')
        (key1, key2) = perm
        result = lambda_client.invoke(
            FunctionName = 'page-history-similarity',
            InvocationType = 'Event',
            Payload = bytes(
                json.dumps({
                'key1': key1,
                'key2': key2,
                }), 
                encoding='utf8'
            )
        )
        print(f'done: {perm}')
        return True


    with ThreadPoolExecutor(max_workers=5000) as executor:
        loop = asyncio.get_event_loop()
        tasks = [
            loop.run_in_executor(
                executor,
                compare_files,
                perm
            )
            for perm in perms
        ]
        for response in await asyncio.gather(*tasks):
            print(response)
            pass

    print(f'[PERM AMOUNT]: {len(perms)}')
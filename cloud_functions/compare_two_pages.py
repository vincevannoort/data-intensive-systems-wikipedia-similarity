import glob
import itertools
import json
import statistics
import boto3
import datetime

def lambda_handler(event, context):
    """
    The lambda handler
    """
    print("Started lambda handler")
    session = boto3.Session()
    s3_client = session.client('s3')

    bucket = 'data-intensive-storage'
    key1 = event['key1']
    key2 = event['key2']

    s3_object_1 = s3_client.get_object(Bucket='data-intensive-storage', Key=key1)
    s3_object_2 = s3_client.get_object(Bucket='data-intensive-storage', Key=key2)

    json_object_1 = json.loads(s3_object_1['Body'].read())
    json_object_2 = json.loads(s3_object_2['Body'].read())

    print("Finished lambda handler")
    return compare_two_files(json_object_1, json_object_2)

def jaccard_similarity(list1, list2):
    intersection = len(list(set(list1).intersection(list2)))
    union = (len(list1) + len(list2)) - intersection

    if (union == 0):
        return 0

    return float(intersection) / union
    

def compare_two_files(file_1_json, file_2_json) -> float:
    """
    Compare two files with their respective number
    """

    similarities = []
    file_1_versions = [version['info']['timestamp'] for version in file_1_json]
    file_2_versions = [version['info']['timestamp'] for version in file_2_json]

    def find_closest_version(file_1_version):
        """
        For a version of file 1
        Find a version of file 2 that is closest in history
        """
        return next((j for j, file_2_version in enumerate(file_2_versions) if file_2_version <= file_1_version), None)

    
    """
    For every version of file 1
    Find a matching version of file 2
    """
    version_pairs = [(file_1_version_index, find_closest_version(file_1_version)) for (file_1_version_index, file_1_version) in enumerate(file_1_versions)]

    def to_date_time(date_time_string):
        return datetime.datetime.strptime(date_time_string, "%Y-%m-%dT%H:%M:%S")

    file_1_first_date = to_date_time(file_1_json[0]['info']['timestamp'])
    file_1_last_date = to_date_time(file_1_json[-1]['info']['timestamp'])
    file_2_first_date = to_date_time(file_2_json[0]['info']['timestamp'])
    file_2_last_date = to_date_time(file_2_json[-1]['info']['timestamp'])
    file_1_total_difference = file_1_first_date - file_1_last_date
    file_2_total_difference = file_2_first_date - file_2_last_date
    # print(file_1_difference.total_minutes())

    for (file_1_version_index, file_2_version_index) in version_pairs:
        if (file_2_version_index is None):
            continue
        # calculate similarity score
        file_1_links = file_1_json[file_1_version_index]['links']
        file_2_links = file_2_json[file_2_version_index]['links']
        similarity = jaccard_similarity(file_1_links, file_2_links)

        file_1_date = to_date_time(file_1_json[file_1_version_index]['info']['timestamp'])
        file_1_difference = file_1_first_date - file_1_date
        print(file_1_difference.total_seconds() / file_1_total_difference.total_seconds())
        # file_2_date = file_2_json[file_2_version_index]['info']['timestamp']
        # file_1_version = file_1_json[file_1_version_index]['links']
        similarities.append(similarity)


    # TODO: do similarities other way around

    if (len(similarities) == 0):
        return 0

    return statistics.mean(similarities)
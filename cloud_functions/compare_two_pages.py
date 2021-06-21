import glob
import itertools
import json
import statistics
import boto3

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


    for (file_1_version_index, file_2_version_index) in version_pairs:
        if (file_2_version_index is None):
            continue
        file_1_links = file_1_json[file_1_version_index]['links']
        file_2_links = file_2_json[file_2_version_index]['links']
        similarity = jaccard_similarity(file_1_links, file_2_links)
        similarities.append(similarity)

    return statistics.mean(similarities)
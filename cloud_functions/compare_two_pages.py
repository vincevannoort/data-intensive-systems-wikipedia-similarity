import glob
import itertools
import json
import statistics
import boto3
from datetime import datetime
from decimal import Decimal

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
    

def compare_two_files(file_1_id, file_2_id, file_1_json, file_2_json) -> float:
    """
    Compare two files with their respective number
    """
    file_1_versions = [version['info']['timestamp'] for version in file_1_json]
    file_2_versions = [version['info']['timestamp'] for version in file_2_json]

    
    def get_similarity_score_single_side(file_a_versions, file_b_versions, file_a_json, file_b_json):
        """
        For every version of file 1
        Find a matching version of file 2
        """

        def find_closest_version(file_a_version):
            """
            For a version of file 1
            Find a version of file 2 that is closest in history
            """
            return next((j for j, file_b_version in enumerate(file_b_versions) if file_b_version <= file_a_version), None)

        def to_date_time(date_time_string):
            return datetime.strptime(date_time_string, "%Y-%m-%dT%H:%M:%S")

        similarities = []
        total_ratio = 0
        version_pairs = [(file_a_version_index, find_closest_version(file_a_version)) for (file_a_version_index, file_a_version) in enumerate(file_a_versions)]

        file_a_last_date = to_date_time(file_1_json[-1]['info']['timestamp'])
        file_b_last_date = to_date_time(file_2_json[-1]['info']['timestamp'])
        second_oldest_date = file_a_last_date if file_a_last_date < file_b_last_date else file_b_last_date

        # compare from start to time now
        now = datetime.now()
        file_a_total_difference = now - file_a_last_date 
        file_b_total_difference = now - file_b_last_date

        def is_not_first_revision(index):
            return index != 0

        for (file_a_version_index, file_b_version_index) in version_pairs:
            if (file_b_version_index is None):
                continue

            # calculate similarity score
            file_a_links = file_a_json[file_a_version_index]['links']
            file_b_links = file_b_json[file_b_version_index]['links']
            similarity = jaccard_similarity(file_a_links, file_b_links)

            file_a_date = to_date_time(file_a_json[file_a_version_index]['info']['timestamp'])
            file_b_next_date = to_date_time(file_a_json[file_a_version_index - 1]['info']['timestamp']) if is_not_first_revision(file_a_version_index) else now
            file_a_difference = file_b_next_date - file_a_date

            # calculate how much of the total time this revision was
            ratio = file_a_difference.total_seconds() / file_a_total_difference.total_seconds()
            total_ratio = total_ratio + ratio
            print(similarity)
            score = ratio * similarity
            similarities.append(score)


        # TODO: do similarities other way around

        if (len(similarities) == 0):
            return 0

        print(f'total_ratio: {total_ratio}')
        # sum the scores, since we already applied the ratio to every each
        return sum(similarities)

    def get_similarity_score_dual_sided(file_a_versions, file_b_versions, file_a_json, file_b_json):
        print('side a')
        side_a_score = get_similarity_score_single_side(file_a_versions, file_b_versions, file_a_json, file_b_json)
        print('side b')
        side_b_score = get_similarity_score_single_side(file_b_versions, file_a_versions, file_b_json, file_a_json)
        print(side_a_score, side_b_score)
        # return (side_a_score + side_b_score) / 2
        return side_a_score

    def get_key(file_1_id, file_2_id):
        # print(file_1_id, file_2_id)
        if (int(file_1_id) < int(file_2_id)):
            return f'{file_1_id}:{file_2_id}'
        else:
            return f'{file_2_id}:{file_1_id}'

    score = get_similarity_score_dual_sided(file_1_versions, file_2_versions, file_1_json, file_2_json)

    return (get_key(file_1_id, file_2_id), Decimal(str(score)))
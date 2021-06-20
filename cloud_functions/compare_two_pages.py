import glob
import itertools
import json
import statistics

def lambda_handler(event, context):
    """
    The lambda handler
    """
    print("Started lambda handler")
    print("Finished lambda handler")
    return {
        'similarity': compare_two_files(event['file1'], event['file2'])
    }

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
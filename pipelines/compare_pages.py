import sys
import glob
import itertools
import json
import statistics
from typing import Tuple, List
from config import settings
from cloud_functions.compare_two_pages import compare_two_files

def get_files_list() -> List[str]:
    return(glob.glob(f'./{settings.data_folder}/*.json'))

def make_permutations(files: List[str]):
    return list(itertools.combinations(files, 2))

def open_files_and_compare(files: Tuple[str, str]):
    (file1, file2) = files
    with open(file1, 'r') as file1_data, open(file2, 'r') as file2_data:
        file_1_json = json.load(file1_data)
        file_2_json = json.load(file2_data)
        score = compare_two_files(file_1_json, file_2_json)
        return score

def compare_files(spark):
    files = get_files_list()
    perms = make_permutations(files)

    parallel_perms = spark.parallelize(perms)
    parallel_perms_map_result = parallel_perms.map(open_files_and_compare)
    print(parallel_perms_map_result.collect())
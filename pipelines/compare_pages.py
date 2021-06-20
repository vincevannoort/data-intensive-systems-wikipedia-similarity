import sys
import glob
import itertools
import json
import statistics
from config import settings
from cloud_functions.compare_two_pages import compare_two_files

def get_files_list() -> list[str]:
    return(glob.glob(f'./{settings.data_folder}/*.json'))

def make_permutations(files: list[str]):
    return list(itertools.product(files, files))

def compare_files():
    files = get_files_list()
    perms = make_permutations(files)

    for (file1, file2) in perms:
         with open(file1, 'r') as file1_data, open(file2, 'r') as file2_data:
            file_1_json = json.load(file1_data)
            file_2_json = json.load(file2_data)
            compare_two_files(file_1_json, file_2_json)
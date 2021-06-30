import json
from pipelines.compare_pages import get_files_list

def calculate_total_revision_count():
    files = get_files_list()
    revisions = 0
    for file in files:
        print(file)
        with open(file, 'r') as data:
            json_data = json.load(data)
            revisions = revisions + len(json_data)
    return revisions
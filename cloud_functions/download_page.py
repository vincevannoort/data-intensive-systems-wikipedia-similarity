"""Usage: my_program.py [-hso FILE] [--quiet | --verbose] [INPUT ...]

-h --help    show this
-s --sorted  sorted output
-o FILE      specify output file [default: ./test.txt]
--quiet      print less text
--verbose    print more text

"""

import mwclient
import json
import time
import re
import os

site = mwclient.Site('en.wikipedia.org')

def lambda_handler(event, context):
    """
    The lambda handler
    """
    return {
        'statusCode': 200,
    }

def get_references_from_page_content(content):
    """
    Get references from content from wikipedia.
    This returns a list of references, ignoring all other text.
    """
    regex_pattern = "\[\[(.*?)\]\]"
    link_regex = re.compile(regex_pattern)
    if '*' in content:
        matches = link_regex.findall(content['*'])
        return matches
    else:
        return []

def download_page(name, force=False):
    """
    Retrieve data and store as json file for a wikipedia page
    """
    page = site.pages[name]

    directory = f"data"
    id = page._info['pageid']
    filename = f"{id}-{name}.json"
    
    revisions = []

    file_location = f"{directory}/{filename}"

    # check if file already exists
    if (os.path.isfile(file_location)) and (not(force)):
        print(f'[EXISTS] {name}')
        return False

    revision_count = len(list(page.revisions()))

    print(f'[START] {name}, with {revision_count} revisions')

    for i, (info, content) in enumerate(zip(page.revisions(), page.revisions(prop='content'))):
        info['timestamp'] = time.strftime("%Y-%m-%dT%H:%M:%S", info['timestamp'])
        links = get_references_from_page_content(content)


        version = {
            'info': info,
            'links': links
        }

        revisions.append(version)

    open(file_location, 'w').write(json.dumps(revisions, indent=2))
    return True

    
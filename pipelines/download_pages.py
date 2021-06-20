import mwclient
# from config import settings
import json
import time
import re

site = mwclient.Site('en.wikipedia.org')
names = [
    'The Seven Pillars of Life',
    'Life',
    'Biological organisation',
    'Carbon-based life',
    'Living systems',
    'Non-cellular life',
    ]

def get_references_from_content(content):
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

def get_data_for_names(names):
    """
    Retrieve data for a list of names from the wikipedia API
    """
    for name in names:
        get_data_for_name(name)

def get_data_for_name(name):
    """
    Retrieve data and store as json file for a wikipedia page
    """
    print(f'[START] {name}')
    page = site.pages[name]

    directory = f"data"
    id = page._info['pageid']
    filename = f"{id}-{name}.json"
    
    revisions = []

    for i, (info, content) in enumerate(zip(page.revisions(), page.revisions(prop='content'))):
        info['timestamp'] = time.strftime("%Y-%m-%dT%H:%M:%S", info['timestamp'])
        links = get_references_from_content(content)

        version = {
            'info': info,
            'links': links
        }

        revisions.append(version)


    open(f"{directory}/{filename}", "w").write(json.dumps(revisions, indent=2))
    print(f'[FINISHED] {name}')
import mwclient
import json
import time
import re
from cloud_functions.download_page import download_page



names = [
    'The Seven Pillars of Life',
    'Life',
    'Biological organisation',
    'Carbon-based life',
    'Living systems',
    'Non-cellular life',
    ]


def download_pages(spark, names):
    """
    Retrieve data for a list of names from the wikipedia API
    """

    parallel_pages = spark.parallelize(names)
    parallel_pages_map_result = parallel_pages.map(download_page)
    print(parallel_pages_map_result.collect())
    # for element in parallel_pages_map_result.collect():
        # print(element)
    # for name in names:
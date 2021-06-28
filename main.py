"""Download pages.

Usage:
  main.py download single <page>
  main.py download multiple (<pages>)
  main.py upload all
  main.py compare local
  main.py compare cloud

Examples:
  main.py download single 'The Seven Pillars of Life'
  main.py download multiple 'The Seven Pillars of Life','Life','Biological organisation','Carbon-based life','Living systems','Non-cellular life'
"""

import asyncio
from config import settings
from pipelines.compare_pages import compare_files_local, compare_files_cloud
from pipelines.download_pages import download_pages
from pipelines.upload_pages import upload_pages
from cloud_functions.download_page import download_page
from docopt import docopt
from pyspark import SparkContext, SparkConf
import boto3

conf = SparkConf().setAppName('WikipediaSimilarity').setMaster('local[*]')
spark = SparkContext(conf=conf)
# spark = None

session = boto3.Session(
    aws_access_key_id = settings['aws_access_key_id'],
    aws_secret_access_key = settings['aws_secret_access_key'],
    aws_session_token = settings['aws_session_token']
)

# compare_files()
import random

if __name__ == '__main__':
    arguments = docopt(__doc__, version='Naval Fate 2.0')
    
    """
    Downloading
    """
    if (arguments['download']):

        if (arguments['single']):
            print('download single page')
            download_page(arguments['<page>'])

        if (arguments['multiple']):
            print('download multiple pages')
            pages = arguments['<pages>'].split(',')
            download_pages(spark, pages)

    """
    Uploading
    """
    if (arguments['upload']):

            if (arguments['all']):
                upload_pages(session)
                # scores = compare_files(spark)
                # print(scores)


    """
    Comparing
    """
    if (arguments['compare']):

            if (arguments['local']):
                scores = compare_files_local(spark, session)
                print(scores)

            if (arguments['cloud']):
                # compare_files_cloud(spark, session)
                loop = asyncio.get_event_loop()
                future = asyncio.ensure_future(compare_files_cloud(spark, session))
                loop.run_until_complete(future)

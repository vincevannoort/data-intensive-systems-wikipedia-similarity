"""Download pages.

Usage:
  main.py download single <page>
  main.py download multiple (<pages>)
  main.py compare all

Examples:
  main.py download single 'The Seven Pillars of Life'
  main.py download multiple 'The Seven Pillars of Life','Life','Biological organisation','Carbon-based life','Living systems','Non-cellular life'
"""


from config import settings
from pipelines.compare_pages import compare_files
from pipelines.download_pages import download_pages
from cloud_functions.download_page import download_page
from docopt import docopt
from pyspark import SparkContext, SparkConf

conf = SparkConf().setAppName('WikipediaSimilarity').setMaster('local[*]')
spark = SparkContext(conf=conf)


# compare_files()
import random

if __name__ == '__main__':
    arguments = docopt(__doc__, version='Naval Fate 2.0')

    # NUM_SAMPLES = 100000
    # def inside(p):
    #   x, y = random.random(), random.random()
    #   return x*x + y*y < 1
    # count = sc.parallelize(range(0, NUM_SAMPLES)).filter(inside).count()
    # pi = 4 * count / NUM_SAMPLES
    # print(f'Pi is roughly: {pi}')

    
    print(arguments)
    if (arguments['download']):

        if (arguments['single']):
            print('download single page')
            download_page(arguments['<page>'])

        if (arguments['multiple']):
            print('download multiple pages')
            pages = arguments['<pages>'].split(',')
            download_pages(spark, pages)

    if (arguments['compare']):

            if (arguments['all']):
                scores = compare_files(spark)
                print(scores)

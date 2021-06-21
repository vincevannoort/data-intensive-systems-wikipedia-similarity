import json
from pipelines.compare_pages import get_files_list
from config import settings

def upload_pages(session):

    s3_client = session.client('s3')
    s3_object = s3_client.get_object(Bucket='data-intensive-storage', Key='16554664-Living systems.json')
    print(json.loads(s3_object['Body'].read()))

    # for my_bucket_object in s3_bucket.objects.all():
        # print(my_bucket_object['Body'].read())
        # print(my_bucket_object)
        # s3_object = s3.get_object(Bucket=bucket_name, Key=key)
        # body = s3_object['Body']

import boto3
import csv

def create_output(session):
    print('start generate output')

    dynamodb_client = session.resource('dynamodb', 'us-east-1')
    names_table = dynamodb_client.Table('data-intensive-names')
    similarity_table = dynamodb_client.Table('data-intensive-database')

    names_scan = names_table.scan()
    similarity_scan = similarity_table.scan()

    with open('output/names.csv', 'w', encoding='UTF8') as f:
        header = ['pageId', 'name']
        writer = csv.writer(f)
        writer.writerow(header)

        for item in names_scan['Items']:
            writer.writerow([item['pageId'], item['name']])

    with open('output/similarities.csv', 'w', encoding='UTF8') as f:
        header = ['pageOneId', 'pageTwoId', 'similarityScore']
        writer = csv.writer(f)
        writer.writerow(header)

        for item in similarity_scan['Items']:
            pageOneId, pageTwoId = item['relationCombinationId'].split(':', 1)
            score = item['score']
            writer.writerow([pageOneId, pageTwoId, score])

    print('done generate output')
import csv
from neo4j import GraphDatabase
from config import settings

# MERGE (n:Person {name: 'Andy', title: 'Developer'})

def create_graph(session):

    # show all pages with their 

    # MATCH (n:Page)-[s:similarity]-(m:Page) 
    # RETURN n,s

    # show all pages with a similarity score higher than 0.1

    # MATCH (n:Page)-[s:similarity]-(m:Page) 
    # WHERE s.score > 0.1
    # RETURN n

    # find highest similarity score node

    # MATCH (n:Page { name: 'Carbon-based life' })-[s:similarity]-(m:Page) 
    # RETURN n, m 
    # ORDER BY m.score DESC
    # LIMIT 1

    # MATCH (n:Page { name: 'Carbon-based life' })-[s:similarity]-(m:Page) 
    # RETURN m
    # ORDER BY m.score DESC
    # LIMIT 1

    minimum_similarity_score = settings['minimum_similarity_score']

    dynamodb_client = session.resource('dynamodb', 'us-east-1')
    names_table = dynamodb_client.Table('data-intensive-names')
    similarity_table = dynamodb_client.Table('data-intensive-database')

    names_scan = names_table.scan()
    similarity_scan = similarity_table.scan()

    driver = GraphDatabase.driver("neo4j://localhost:7687", auth=("neo4j", "dis"))

    def add_page(tx, id, name):
        tx.run("MERGE ( a:Page { id: $id, name: $name })", id = id, name = name)

    def add_relation(tx, p1, p2, score):
        tx.run(
            "MATCH (a: Page { id: $p1 })"
            "MATCH (b: Page { id: $p2 })"
            "MERGE (a) <- [:similarity { score: $score }] -> (b)",
            p1 = p1,
            p2 = p2,
            score = score
        )

    with driver.session() as session:
        # add pages to neo4j
        for item in names_scan['Items']:
            pageId = item['pageId']
            name = item['name']
            session.write_transaction(add_page, pageId, name)

        for item in similarity_scan['Items']:
            pageOneId, pageTwoId = item['relationCombinationId'].split(':', 1)
            score = item['score']
            if (score >= minimum_similarity_score):
                session.write_transaction(add_relation, pageOneId, pageTwoId, float(score))

    driver.close()
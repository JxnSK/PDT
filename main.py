#from sys import exec_prefix
import psycopg2
import jsonlines
import gzip
from datetime import datetime

def replace_0x00(value):
    return value.replace("\x00","")

def clean_str(value):
    return value.replace("'", "''")

def parse_authors(file, authors_check):
    with gzip.open(file) as f:
        reader = jsonlines.Reader(f)
        for index, obj in enumerate(reader):
            if not obj['id'] in authors_check:
                cursor.execute("INSERT INTO authors VALUES(%s, %s, %s, %s, %s, %s, %s, %s)",
                    (obj['id'], replace_0x00(obj['name']), replace_0x00(obj['username']), replace_0x00(obj['description']), obj['public_metrics']['followers_count'], obj['public_metrics']['following_count'], obj['public_metrics']['tweet_count'], obj['public_metrics']['listed_count'])
                )
                authors_check[obj['id']] = True
            if not index % 100000:
                conn.commit()
                print ('100k jsons processed : {}'.format(datetime.now() - start_time))
    conn.commit()

def parse_conversations(file, authors_check):
    with gzip.open(file) as f:
        reader = jsonlines.Reader(f)
        hashtag_count = 0
        annotation_count = 0
        url_count = 0
        conversation_hashtag_count = 0
        conversations_check = {}
        hashtags_check = {}
        context_entities_check = {}
        context_domains_check = {}

        for index, obj in enumerate(reader):
            if not obj['id'] in conversations_check:
                conversations_check[obj['id']] = True
                #add conversations
                cursor.execute("SELECT * FROM authors WHERE id={}".format(obj['author_id']))
                if cursor.fetchone() == None:
                #if not obj['author_id'] in authors_check:
                #    authors_check[obj['author_id']] = True
                    cursor.execute("INSERT INTO authors VALUES(%s, %s, %s, %s, %s, %s, %s, %s)",
                        (obj['author_id'], None, None, None, None, None, None, None)
                    )
                    conn.commit()
                
                cursor.execute(
                    "INSERT INTO conversations VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)",
                    (
                        obj['id'], obj['author_id'], obj['text'], obj['possibly_sensitive'], obj['lang'], obj['source'], obj['public_metrics']['retweet_count'], 
                        obj['public_metrics']['reply_count'], obj['public_metrics']['like_count'], obj['public_metrics']['quote_count'], obj['created_at']
                    )
                )

                #add hashtags
                try:
                    #previous_hashtag_count = hashtag_count
                    execute = False
                    query = "INSERT INTO hashtags VALUES "
                    for hashtag_obj in obj['entities']['hashtags']:
                        if not hashtag_obj['tag'] in hashtags_check:
                            execute = True
                            query += "({}, '{}'),".format(hashtag_count, hashtag_obj['tag'])
                            hashtag_count += 1
                            hashtags_check[hashtag_obj['tag']] = True
                    if execute:
                        query = query[:-1] + ';'
                        cursor.execute(query)
                except KeyError:
                    pass

                #add context entities + domains
                try:
                    execute_entity = False
                    execute_domain = False
                    query_entities = "INSERT INTO context_entities VALUES "
                    query_domains = "INSERT INTO context_domains VALUES "

                    for context in obj['context_annotations']:
                        if not context['domain']['id'] in context_domains_check:
                            execute_domain = True
                            try:
                                query_domains += "({}, '{}', '{}'),".format(context['domain']['id'], clean_str(context['domain']['name']), clean_str(context['domain']['description']))
                            except KeyError:
                                #print("MAL SOM KEY ERROR")
                                query_domains += "({}, '{}', '{}'),".format(context['domain']['id'], clean_str(context['domain']['name']), None)
                            context_domains_check[context['domain']['id']] = True
                        
                        if not context['entity']['id'] in context_entities_check:
                            execute_entity = True
                            try:
                                query_entities += "({}, '{}', '{}'),".format(context['entity']['id'], clean_str(context['entity']['name']), clean_str(context['entity']['description']))
                            except KeyError:
                                #print("MAL SOM KEY ERROR")
                                query_entities += "({}, '{}', '{}'),".format(context['entity']['id'], clean_str(context['entity']['name']), None)
                            context_entities_check[context['entity']['id']] = True
                    
                    if execute_domain:
                        query_domains = query_domains[:-1] + ';'
                        cursor.execute(query_domains)
                    if execute_entity:
                        query_entities = query_entities[:-1] + ';'
                        cursor.execute(query_entities)
                except KeyError:
                    #print('ERROR IN CONTEXT ENTITIES+DOMAINS!')
                    #print(obj)
                    pass

                #add annotations
                try:
                    execute = False
                    query = "INSERT INTO annotations VALUES "
                    for annotation_obj in obj['entities']['annotations']:
                        execute = True
                        query += "({}, {}, '{}', '{}', {}),".format(annotation_count, obj['id'], clean_str(annotation_obj['normalized_text']),
                            clean_str(annotation_obj['type']), annotation_obj['probability']
                        )
                        annotation_count += 1
                    if execute:
                        query = query[:-1] + ';'
                        cursor.execute(query)
                except KeyError:
                    pass

                #add links
                try:
                    execute = False
                    query = "INSERT INTO links VALUES "
                    for urls_obj in obj['entities']['urls']:
                        if len(urls_obj['expanded_url']) > 2048:
                            continue
                        execute = True
                        query += "({}, {}, '{}', '{}', '{}'),".format(url_count, obj['id'], clean_str(urls_obj['expanded_url']), clean_str(urls_obj['title']),
                            clean_str(urls_obj['description'])
                        )
                        url_count += 1
                    if execute:
                        query = query[:-1] + ';'
                        cursor.execute(query)
                except KeyError:
                    pass
        if index % 100000 == 0:
                conn.commit()
                print ('100k jsons processed : {}'.format(datetime.now() - start_time))
        #2ND RUN -- TERAZ IBA TOTO
        conn.commit()
        reader = jsonlines.Reader(f)
        del authors_check
        conversations_check = {}
        reference_count = 0
        conversation_hashtag_count = 0
        context_annotations_count = 0
        for obj in reader:
            if not obj['id'] in conversations_check:
                #add conversation_references
                conversations_check[obj['id']] = True
                try:
                    execute = False
                    query = "INSERT INTO conversation_references VALUES "
                    for reference_obj in obj['entities']['referenced_tweets']:
                        execute = True
                        query += "({}, {}, {}, '{}'),".format(reference_count, obj['id'], reference_obj['id'], reference_obj['type'])
                        reference_count += 1
                    if execute:
                        query = query[:-1] + ';'
                        cursor.execute(query)
                except KeyError:
                    pass
                #add N:N mapping of hashtag
                try:
                    execute = False
                    query = "INSERT INTO conversation_hashtags VALUES "
                    for hashtag_obj in obj['entities']['hashtags']:
                            cursor.execute("SELECT id FROM hashtags WHERE tag='{}'".format(hashtag_obj['tag']))
                            execute = True
                            query += "({}, {}, {}),".format(conversation_hashtag_count, obj['id'], cursor.fetchone()[0])
                            conversation_hashtag_count += 1
                    if execute:
                        query = query[:-1] + ';'
                        cursor.execute(query)
                except KeyError:
                    pass
                #add N:N mapping of context_annotation
                try:
                    execute = False
                    query = "INSERT INTO context_annotations VALUES "
                    for context in obj['context_annotations']:
                        execute = True
                        query += "({}, {}, {}, {}),".format(context_annotations_count, obj['id'], context['domain']['id'], context['entity']['id'])
                        context_annotations_count += 1
                    if execute:
                        query = query[:-1] + ';'
                        cursor.execute(query)
                except KeyError:
                    pass
            if not index % 100000:
                conn.commit()
                print ('100k jsons processed : {}'.format(datetime.now() - start_time))

def second_run(file):
    with gzip.open(file) as f:
        reader = jsonlines.Reader(f)
        #del authors_check
        conversations_check = {}
        reference_count = 0
        conversation_hashtag_count = 0
        context_annotations_count = 0
        for index, obj in enumerate(reader):
            if not obj['id'] in conversations_check:
                #add conversation_references
                conversations_check[obj['id']] = True
                try:
                    execute = False
                    query = "INSERT INTO conversation_references VALUES "
                    for reference_obj in obj['entities']['referenced_tweets']:
                        execute = True
                        query += "({}, {}, {}, '{}'),".format(reference_count, obj['id'], reference_obj['id'], reference_obj['type'])
                        reference_count += 1
                    if execute:
                        query = query[:-1] + ';'
                        cursor.execute(query)
                except KeyError:
                    pass
                #add N:N mapping of hashtag
                try:
                    execute = False
                    query = "INSERT INTO conversation_hashtags VALUES "
                    for hashtag_obj in obj['entities']['hashtags']:
                            cursor.execute("SELECT id FROM hashtags WHERE tag='{}'".format(hashtag_obj['tag']))
                            execute = True
                            query += "({}, {}, {}),".format(conversation_hashtag_count, obj['id'], cursor.fetchone()[0])
                            conversation_hashtag_count += 1
                    if execute:
                        query = query[:-1] + ';'
                        cursor.execute(query)
                except KeyError:
                    pass
                #add N:N mapping of context_annotation
                try:
                    execute = False
                    query = "INSERT INTO context_annotations VALUES "
                    for context in obj['context_annotations']:
                        execute = True
                        query += "({}, {}, {}, {}),".format(context_annotations_count, obj['id'], context['domain']['id'], context['entity']['id'])
                        context_annotations_count += 1
                    if execute:
                        query = query[:-1] + ';'
                        cursor.execute(query)
                except KeyError:
                    pass
            if not index % 100000:
                conn.commit()
                print ('100k jsons processed : {}'.format(datetime.now() - start_time))


#main

conn = psycopg2.connect(
    database="tweet_conversations",
    host="localhost",
    user="postgres",
    password="secret",
    port="5432"
)
conn.autocommit = False
cursor = conn.cursor()

start_time = datetime.now()

authors_check = {}

#parse_authors('./data/authors.jsonl.gz', authors_check)
parse_conversations('./data/conversations.jsonl.gz', authors_check)
#second_run('./data/conversations.jsonl.gz')

conn.commit()

end_time= datetime.now()
print('OVERALL DURATION: {}'.format(end_time - start_time))

print()

cursor.close()
conn.close()
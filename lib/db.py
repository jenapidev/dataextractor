import psycopg2
from pymongo import MongoClient
from bson import ObjectId

def db_connection():
    conn = None
    try:
        conn = psycopg2.connect("dbname='%s' user='%s' host='%s' password='%s'" % ('verto-rails_development', 'postgres', 'localhost', '0409'))
    except:
        print("there was a problem with the database connection")
    return conn

def fetch_postgres_data(conn, query):
    result = []
    sys.stdout.write("Now executing: %s" % (query))
    cursor = conn.cursor()
    cursor.execute(query)

    raw = cursor.fetchall()
    for line in raw:
        result.append(line)

    return result


def getFormsCollection(client):
    '''
        returns: parsedForms collections from play verto parsed data databases
    '''
    db = client.vertoDatabase
    parsedForms = db.parsedForms
    return parsedForms

def fetch_clean_data(client, story_id):
    '''
        client: mongo client
        story_id: survey Id from postgres db
        returns: None or object from play verto parsed data db
    '''
    collection = getFormsCollection(client)
    return collection.find_one({ "storyId": story_id })
    

def save_clean_data(client, data):
    '''
        client: mongo client
        returns: None or object from play verto parsed data db
    '''
    collection = getFormsCollection(client)
    return collection.insert_one(data)

def update_clean_data(client, story_id, data):
    '''
        client: mongo client
        story_id: story parsed data object Id
    '''
    collection = getFormsCollection(client)
    collection.update({
        '_id': ObjectId(story_id)
    },{
        '$set': data
    })

def db_cleansed_data_db():
    client = None
    try:
        client = MongoClient('mongodb://{}:27017/'.format(os.environ['DB_HOST'] ))
    except:
        print("there was a problem with the database connection")
    return client
import yaml
import pymongo
from pymongo.results import DeleteResult


def get_conn(db):
    client = pymongo.MongoClient(db['host'], db['port'])
    collect = client.get_database(db['name']).get_collection(db['collectionname'])
    return collect


def delete_by_key(conn: pymongo.collection.Collection, keys):
    r = conn.delete_many({"post_time": {"$in": keys}})
    return r


if __name__ == '__main__':
    with open("config.yaml", "r", encoding="utf-8") as f:
        config = yaml.load(f.read(), Loader=yaml.FullLoader)
    conn = get_conn(config['db'])
    res:DeleteResult = delete_by_key(conn, config['del_key'])
    print(res.deleted_count)

from pymongo import MongoClient


class MongoDBClient:
    def __init__(self, db_name, collection_name, host='localhost', port=27017):
        self.client = MongoClient(host, port)
        self.db = self.client[db_name]
        self.collection = self.db[collection_name]

    def create(self, data):
        """ Create a new document in the collection """
        return self.collection.insert_one(data).inserted_id

    def read(self, query):
        """ Read documents from the collection """
        return list(self.collection.find(query))

    def update(self, query, new_values):
        """ Update documents in the collection """
        return self.collection.update_many(query, {'$set': new_values})

    def delete(self, query):
        """ Delete documents from the collection """
        return self.collection.delete_many(query)

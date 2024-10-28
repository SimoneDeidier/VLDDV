from DbConnector import DbConnector
 
def main():
    
    connection = DbConnector()
    db = connection.db
    
    for collection in db.list_collection_names():
        db.drop_collection(collection)
    
    connection.close_connection()
    
if __name__ == "__main__":
    main()
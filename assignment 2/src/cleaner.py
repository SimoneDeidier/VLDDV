from DbConnector import DbConnector

def main():
    db = DbConnector()
    queries = [
        "DROP TABLE TrackPoint",
        "DROP TABLE Activity",
        "DROP TABLE User"
    ]
    
    print("Cleaning database...")
    for query in queries:
        db.cursor.execute(query) 
    db.db_connection.commit()
    
    print("Database cleaned!")
    
if __name__ == '__main__':
    main()
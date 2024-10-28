from pprint import pprint 
from DbConnector import DbConnector
import time
import os
import datetime

def parse_date(date_str, time_str):
        try:
            return datetime.datetime.strptime(date_str + ' ' + time_str, '%Y/%m/%d %H:%M:%S')
        except ValueError:
            return datetime.datetime.strptime(date_str + ' ' + time_str, '%Y-%m-%d %H:%M:%S')

def main():
    
    # Task 1.1: connect to the DB
    connection = DbConnector()
    db = connection.db
    
    # Task 1.2: create collections
    print("Creating collections...")
    
    start_timing = time.time()
    # Check if the User collection exists, if not create it
    if "User" not in db.list_collection_names():
        db.create_collection("User")
    # Check if the Activity collection exists, if not create it
    if "Activity" not in db.list_collection_names():
        db.create_collection("Activity")
    # Check if the TrackPoint collection exists, if not create it
    if "TrackPoint" not in db.list_collection_names():
        db.create_collection("TrackPoint")
    end_timing = time.time()
    print(f"Collections created in {end_timing - start_timing:.2f} seconds!\n")
    
    # Task 1.3: insert documents
    print("Inserting documents...\n\tInserting User documents...")
    
    start_timing = time.time()
    # Check if the User collection is empty
    if db.User.count_documents({}) == 0:
        dataset_folder = 'dataset/Data'
        labeled_ids_file = 'dataset/labeled_ids.txt'
        # Read labeled_ids from file
        with open(labeled_ids_file, 'r') as file:
            labeled_ids = set(file.read().splitlines())

        # Insert user documents into the User collection
        users = []
        activities = []
        for folder_name in os.listdir(dataset_folder):
            folder_path = os.path.join(dataset_folder, folder_name)
            if os.path.isdir(folder_path):
                has_labels = folder_name in labeled_ids
                trajectory_folder_path = os.path.join(folder_path, 'Trajectory')
                for plt_file_name in os.listdir(trajectory_folder_path):
                    plt_file_path = os.path.join(trajectory_folder_path, plt_file_name)
                    with open(plt_file_path, 'r') as plt_file:
                        lines = plt_file.readlines()[6:]
                        if len(lines) < 2500:
                            activities.append({"start_date_time": parse_date(*lines[0].strip().split(',')[5:7])})
                users.append({"_id": folder_name, "has_labels": has_labels, "activities": activities})

        # Insert user documents into the User collection
        db.User.insert_many(users)
    end_timing = time.time()
    
    print(f"\tUser documents inserted in {end_timing - start_timing:.2f} seconds!\n\tInserting Activity documents...")

    # start_timing = time.time()
        
    # Check if the Activity collection is empty
    '''if db.Activity.count_documents({}) == 0:
        for user_id in os.listdir(dataset_folder):
            user_folder_path = os.path.join(dataset_folder, user_id)
            if os.path.isdir(user_folder_path):
                labels = {}
                # Read labels for users with labeled data
                if user_id in labeled_ids:
                    labels_file_path = os.path.join(user_folder_path, 'labels.txt')
                    with open(labels_file_path, 'r') as labels_file:
                        next(labels_file)
                        for line in labels_file:
                            start_time, end_time, mode = line.strip().split('\t')
                            labels[(start_time, end_time)] = mode

                trajectory_folder_path = os.path.join(user_folder_path, 'Trajectory')
                for plt_file_name in os.listdir(trajectory_folder_path):
                    if plt_file_name.endswith('.plt'):
                        plt_file_path = os.path.join(trajectory_folder_path, plt_file_name)
                        with open(plt_file_path, 'r') as plt_file:
                            lines = plt_file.readlines()[6:]
                            if len(lines) < 2500:
                                start_date_time = parse_date(*lines[0].strip().split(',')[5:7])
                                end_date_time = parse_date(*lines[-1].strip().split(',')[5:7])
                                transportation_mode = None
                                # Match activity with labels
                                if user_id in labeled_ids:
                                    for (start_time, end_time), mode in labels.items():
                                        if start_date_time == parse_date(*start_time.split()) and end_date_time == parse_date(*end_time.split()):
                                            transportation_mode = mode
                                            break
                                query = """INSERT INTO Activity (user_id, transportation_mode, start_date_time, end_date_time)
                                           VALUES (%s, %s, %s, %s)"""
                                db.cursor.execute(query, (user_id, transportation_mode, start_date_time, end_date_time))
        
        db.db_connection.commit()
    end_timing = time.time()'''

    connection.close_connection()

if __name__ == '__main__':
    main()

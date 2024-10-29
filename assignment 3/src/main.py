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
    dataset_folder = 'dataset/Data'
    labeled_ids = set()
    labeled_ids_file = 'dataset/labeled_ids.txt'
    # Check if the User collection is empty
    if db.User.count_documents({}) == 0:
        # Read labeled_ids from file
        with open(labeled_ids_file, 'r') as file:
            labeled_ids = set(file.read().splitlines())

        # Insert user documents into the User collection
        users = []
        activities = []
        activity_index = 0
        for folder_name in sorted(os.listdir(dataset_folder)):
            folder_path = os.path.join(dataset_folder, folder_name)
            if os.path.isdir(folder_path):
                has_labels = folder_name in labeled_ids
                trajectory_folder_path = os.path.join(folder_path, 'Trajectory')
                for plt_file_name in sorted(os.listdir(trajectory_folder_path)):
                    plt_file_path = os.path.join(trajectory_folder_path, plt_file_name)
                    with open(plt_file_path, 'r') as plt_file:
                        lines = plt_file.readlines()[6:]
                        if len(lines) < 2500:
                            activities.append({"activity_id": activity_index})
                            activity_index += 1
                users.append({"_id": folder_name, "has_labels": has_labels, "activities": activities})
                activities = []
        # Insert user documents into the User collection
        db.User.insert_many(users)
    end_timing = time.time()
    
    print(f"\tUser documents inserted in {end_timing - start_timing:.2f} seconds!\n\tInserting Activity documents...")

    start_timing = time.time()
    # Check if the Activity collection is empty
    if db.Activity.count_documents({}) == 0:
        index = 0
        activities = []
        for user_id in sorted(os.listdir(dataset_folder)):
            user_folder_path = os.path.join(dataset_folder, user_id)
            if os.path.isdir(user_folder_path):
                labels = {}
                # Read labels for users with labeled data
                if len(labeled_ids) <= 0:
                    with open(labeled_ids_file, 'r') as file:
                        labeled_ids = set(file.read().splitlines())
                if user_id in labeled_ids:
                    labels_file_path = os.path.join(user_folder_path, 'labels.txt')
                    with open(labels_file_path, 'r') as labels_file:
                        next(labels_file)
                        for line in labels_file:
                            start_time, end_time, mode = line.strip().split('\t')
                            labels[(start_time, end_time)] = mode

                trajectory_folder_path = os.path.join(user_folder_path, 'Trajectory')
                for plt_file_name in sorted(os.listdir(trajectory_folder_path)):
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
                                activities.append({"_id": index, "transportation_mode": transportation_mode, "start_date_time": start_date_time.isoformat(), "end_date_time": end_date_time.isoformat()})
                        index += 1
        # Insert activity documents into the Activity collection
        db.Activity.insert_many(activities)
    end_timing = time.time()
    
    print(f"\tActivity documents inserted in {end_timing - start_timing:.2f} seconds!\n\tInserting TrackPoint documents...")
    
    start_timing = time.time()
    # Check if the TrackPoint collection is empty
    if db.TrackPoints.count_documents({}) == 0:
        index = 0
        track_points = []
        for user_id in sorted(os.listdir(dataset_folder)):
            user_folder_path = os.path.join(dataset_folder, user_id)
            if os.path.isdir(user_folder_path):
                trajectory_folder_path = os.path.join(user_folder_path, 'Trajectory')
                for plt_file_name in sorted(os.listdir(trajectory_folder_path)):
                    if plt_file_name.endswith('.plt'):
                        plt_file_path = os.path.join(trajectory_folder_path, plt_file_name)
                        with open(plt_file_path, 'r') as plt_file:
                            lines = plt_file.readlines()[6:]
                            if len(lines) < 2500:
                                start_date_time = parse_date(*lines[0].strip().split(',')[5:7]).isoformat()
                                end_date_time = parse_date(*lines[-1].strip().split(',')[5:7]).isoformat()
                                
                                user_activities = db.User.find_one({"_id": user_id})["activities"]
                                user_activities_id = [a["activity_id"] for a in user_activities]
                                activities = db.Activity.find({"start_date_time": start_date_time, "end_date_time": end_date_time})
                                
                                activity_id = None
                                for a in activities:
                                    if a["_id"] in user_activities_id:
                                        activity_id = a["_id"]
                                        break
                                    
                                if activity_id is None:
                                    print(f"[ ERROR ]: Activity not found for user {user_id} with start date time {start_date_time} and end date time {end_date_time}")
                                    exit(1)
                                    
                                for line in lines:
                                    lat, lon, _, altitude, date_days, date, time_str = line.strip().split(',')
                                    track_points.append({"_id": index, "lat": float(lat), "lon": float(lon), "altitude": int(float(altitude)), "date_days": float(date_days), "date_time": parse_date(date, time_str).isoformat(), "activity_id": activity_id})
                                    index += 1
        # Insert track point documents into the TrackPoint collection
        db.TrackPoint.insert_many(track_points)
        
    print(f"[ DEBUG ]: Printing the first User, Activity and TrackPoint documents...")
    pprint(db.User.find_one())
    pprint(db.Activity.find_one())
    pprint(db.TrackPoint.find_one())

    connection.close_connection()

if __name__ == '__main__':
    main()

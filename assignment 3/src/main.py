from pprint import pprint 
from DbConnector import DbConnector
import time
import os
import datetime
import haversine

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
    print(f"Collections created in {end_timing - start_timing:.2f} seconds!")
    
    # Task 1.3: insert documents
    print("Inserting documents...\n\tInserting User documents...")
    
    start_timing = time.time()
    dataset_folder = 'dataset/Data'
    labeled_ids = set()
    labeled_ids_file = 'dataset/labeled_ids.txt'
    plt_to_index = {}
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
                            plt_to_index[plt_file_name] = activity_index
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
    if db.TrackPoint.count_documents({}) == 0:
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
                                for line in lines:
                                    lat, lon, _, altitude, date_days, date, time_str = line.strip().split(',')
                                    track_points.append({"_id": index, "lat": float(lat), "lon": float(lon), "altitude": int(float(altitude)), "date_days": float(date_days), "date_time": parse_date(date, time_str).isoformat(), "activity_id": plt_to_index[plt_file_name]})
                                    index += 1
        # Insert track point documents into the TrackPoint collection
        db.TrackPoint.insert_many(track_points)
    end_timing = time.time()
    
    print(f"\tTrackPoint documents inserted in {end_timing - start_timing:.2f} seconds!\nData inserted!")

    print("\n\n---------QUERIES---------\n")
    
    # Task 2.1: Count the number of users, activities, and trackpoints in the dataset
    start_timing = time.time()
    num_users = db.User.count_documents({})
    num_activities = db.Activity.count_documents({})
    num_trackpoints = db.TrackPoint.count_documents({})
    end_timing = time.time() 
    print(f"\nTask 2.1: How many users, activities and trackpoints are there in the dataset? (Executed in {end_timing - start_timing:.2f} seconds)")
    print(f"Number of users: {num_users}")
    print(f"Number of activities: {num_activities}")
    print(f"Number of trackpoints: {num_trackpoints}")
    
    # Task 2.2: Find the average number of activities per user
    start_timing = time.time()
    pipeline = [
        {"$unwind": "$activities"},
        {"$group": {"_id": "$_id", "num_activities": {"$sum": 1}}},
        {"$group": {"_id": None, "avg_activities_per_user": {"$avg": "$num_activities"}}}
    ]
    result = list(db.User.aggregate(pipeline))
    avg_activities_per_user = result[0]['avg_activities_per_user'] if result else 0
    end_timing = time.time()
    print(f"\nTask 2.2: Find the average number of activities per user (Executed in {end_timing - start_timing:.2f} seconds)")
    print(f"Average number of activities per user: {avg_activities_per_user:.2f}")
    
    # Task 2.3: Find the top 20 users with the highest number of activities
    start_timing = time.time()
    pipeline = [
        {"$unwind": "$activities"},
        {"$group": {"_id": "$_id", "num_activities": {"$sum": 1}}},
        {"$sort": {"num_activities": -1}},
        {"$limit": 20}
    ]
    top_users = list(db.User.aggregate(pipeline))
    end_timing = time.time()
    print(f"\nTask 2.3: Find the top 20 users with the highest number of activities (Executed in {end_timing - start_timing:.2f} seconds)")
    pprint(top_users)
    
    # Task 2.4: Find all users who have taken a taxi
    start_timing = time.time()
    taxi_users = db.Activity.distinct("user_id", {"transportation_mode": "taxi"})
    end_timing = time.time()
    print(f"\nTask 2.4: Find all users who have taken a taxi (Executed in {end_timing - start_timing:.2f} seconds)")
    pprint(taxi_users)
    
    # Task 2.5: Find all types of transportation modes and count how many activities are tagged with these transportation mode labels
    start_timing = time.time()
    pipeline = [
        {"$match": {"transportation_mode": {"$ne": None}}},
        {"$group": {"_id": "$transportation_mode", "count": {"$sum": 1}}},
        {"$sort": {"count": -1}}
    ]
    transportation_modes = list(db.Activity.aggregate(pipeline))
    end_timing = time.time()
    print(f"\nTask 2.5: Find all types of transportation modes and count how many activities are tagged with these transportation mode labels (Executed in {end_timing - start_timing:.2f} seconds)")
    pprint(transportation_modes)
    
    # Task 2.6: Find the year with the most activities and check if it is also the year with the most recorded hours
    start_timing = time.time()
    pipeline = [
        {"$group": {"_id": {"$year": {"$dateFromString": {"dateString": "$start_date_time"}}}, "count": {"$sum": 1}}},
        {"$sort": {"count": -1}},
        {"$limit": 1}
    ]
    most_activities_year = list(db.Activity.aggregate(pipeline))[0]['_id']

    pipeline = [
        {"$project": {"year": {"$year": {"$dateFromString": {"dateString": "$start_date_time"}}}, "duration": {"$subtract": [{"$dateFromString": {"dateString": "$end_date_time"}}, {"$dateFromString": {"dateString": "$start_date_time"}}]}}},
        {"$group": {"_id": "$year", "total_duration": {"$sum": "$duration"}}},
        {"$sort": {"total_duration": -1}},
        {"$limit": 1}
    ]
    most_recorded_hours_year = list(db.Activity.aggregate(pipeline))[0]['_id']
    end_timing = time.time()

    print(f"\nTask 2.6: Find the year with the most activities and check if it is also the year with the most recorded hours (Executed in {end_timing - start_timing:.2f} seconds)")
    print(f"Year with the most activities: {most_activities_year}")
    print(f"Year with the most recorded hours: {most_recorded_hours_year}")
    
    # Task 2.7: Find the total distance walked in 2008 by user with id=112
    start_timing = time.time()
    user_id = "112"
    total_distance = 0.0

    # Find all walking activities for user 112 in 2008
    walking_activities = db.Activity.find({
        "user_id": user_id,
        "transportation_mode": "walk",
        "start_date_time": {"$gte": "2008-01-01T00:00:00", "$lt": "2009-01-01T00:00:00"}
    })

    for activity in walking_activities:
        track_points = db.TrackPoint.find({"activity_id": activity["_id"]}).sort("date_time", 1)
        prev_point = None
        for point in track_points:
            if prev_point is not None:
                total_distance += haversine.haversine(
                    (prev_point["lat"], prev_point["lon"]),
                    (point["lat"], point["lon"])
                )
            prev_point = point

    end_timing = time.time()
    print(f"\nTask 2.7: Find the total distance walked in 2008 by user with id=112 (Executed in {end_timing - start_timing:.2f} seconds)")
    print(f"Total distance walked in 2008 by user 112: {total_distance:.2f} km")
    
    # Task 2.8: Find the top 20 users who have gained the most altitude meters

    connection.close_connection()

if __name__ == '__main__':
    main()
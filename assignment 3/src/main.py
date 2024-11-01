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
                                activities.append({"_id": index, "transportation_mode": transportation_mode, "start_date_time": start_date_time.isoformat(), "end_date_time": end_date_time.isoformat(), "user_id": user_id})
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
    
    # Print the first 10 documents of User, Activity, and TrackPoint collections
    print("\nFirst 10 User documents:")
    pprint(list(db.User.find().limit(10)))

    print("\nFirst 10 Activity documents:")
    pprint(list(db.Activity.find().limit(10)))

    print("\nFirst 10 TrackPoint documents:")
    pprint(list(db.TrackPoint.find().limit(10)))

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
    # Pipeline to calculate the average number of activities per user
    pipeline = [
        {"$unwind": "$activities"},  # Deconstructs the activities array field from the input documents to output a document for each element
        {"$group": {"_id": "$_id", "num_activities": {"$sum": 1}}},  # Groups by user ID and counts the number of activities per user
        {"$group": {"_id": None, "avg_activities_per_user": {"$avg": "$num_activities"}}}  # Calculates the average number of activities per user
    ]
    result = list(db.User.aggregate(pipeline))
    avg_activities_per_user = result[0]['avg_activities_per_user'] if result else 0
    end_timing = time.time()
    print(f"\nTask 2.2: Find the average number of activities per user (Executed in {end_timing - start_timing:.2f} seconds)")
    print(f"Average number of activities per user: {avg_activities_per_user:.2f}")
    
    # Task 2.3: Find the top 20 users with the highest number of activities
    start_timing = time.time()
    # Pipeline to find the top 20 users with the highest number of activities
    pipeline = [
        {"$unwind": "$activities"},  # Deconstructs the activities array field from the input documents to output a document for each element
        {"$group": {"_id": "$_id", "num_activities": {"$sum": 1}}},  # Groups by user ID and counts the number of activities per user
        {"$sort": {"num_activities": -1}},  # Sorts the users by the number of activities in descending order
        {"$limit": 20}  # Limits the result to the top 20 users
    ]
    top_users = list(db.User.aggregate(pipeline))
    end_timing = time.time()
    print(f"\nTask 2.3: Find the top 20 users with the highest number of activities (Executed in {end_timing - start_timing:.2f} seconds)")
    pprint(top_users)
    
    # Task 2.4: Find all users who have taken a taxi
    start_timing = time.time()
    # Pipeline to find all users who have taken a taxi
    pipeline = [
        {"$lookup": {
            "from": "Activity",
            "localField": "_id",
            "foreignField": "user_id",
            "as": "activities"
        }},  # Join with the Activity collection to get activities for each user
        {"$unwind": "$activities"},  # Deconstructs the activities array field from the input documents to output a document for each element
        {"$match": {"activities.transportation_mode": "taxi"}},  # Filter activities to find those with transportation_mode as "taxi"
        {"$group": {"_id": "$_id"}}  # Group by user ID to get distinct users
    ]
    taxi_users = list(db.User.aggregate(pipeline))
    taxi_users.sort(key=lambda x: x['_id'])
    end_timing = time.time()
    print(f"\nTask 2.4: Find all users who have taken a taxi (Executed in {end_timing - start_timing:.2f} seconds)")
    pprint(taxi_users)
    
    # Task 2.5: Find all types of transportation modes and count how many activities are tagged with these transportation mode labels
    start_timing = time.time()
    # Pipeline to find all types of transportation modes and count how many activities are tagged with these transportation mode labels
    pipeline = [
        {"$match": {"transportation_mode": {"$ne": None}}},  # Filter activities to find those with a non-null transportation_mode
        {"$group": {"_id": "$transportation_mode", "count": {"$sum": 1}}},  # Group by transportation_mode and count the number of activities for each mode
        {"$sort": {"count": -1}}  # Sort the results by count in descending order
    ]
    transportation_modes = list(db.Activity.aggregate(pipeline))
    end_timing = time.time()
    print(f"\nTask 2.5: Find all types of transportation modes and count how many activities are tagged with these transportation mode labels (Executed in {end_timing - start_timing:.2f} seconds)")
    pprint(transportation_modes)
    
    # Task 2.6: Find the year with the most activities and check if it is also the year with the most recorded hours
    start_timing = time.time()
    
    # Pipeline to find the year with the most activities
    pipeline = [
        {"$group": {"_id": {"$year": {"$dateFromString": {"dateString": "$start_date_time"}}}, "count": {"$sum": 1}}},  # Group by year and count the number of activities per year
        {"$sort": {"count": -1}},  # Sort the results by count in descending order
        {"$limit": 1}  # Limit the result to the top year
    ]
    most_activities_year = list(db.Activity.aggregate(pipeline))[0]['_id']
    
    # Pipeline to find the year with the most recorded hours
    pipeline = [
        {"$project": {"year": {"$year": {"$dateFromString": {"dateString": "$start_date_time"}}}, "duration": {"$subtract": [{"$dateFromString": {"dateString": "$end_date_time"}}, {"$dateFromString": {"dateString": "$start_date_time"}}]}}},  # Project the year and duration of each activity
        {"$group": {"_id": "$year", "total_duration": {"$sum": "$duration"}}},  # Group by year and sum the total duration of activities per year
        {"$sort": {"total_duration": -1}},  # Sort the results by total duration in descending order
        {"$limit": 1}  # Limit the result to the top year
    ]
    most_recorded_hours_year = list(db.Activity.aggregate(pipeline))[0]['_id']
    
    end_timing = time.time()
    print(f"\nTask 2.6: Find the year with the most activities and check if it is also the year with the most recorded hours (Executed in {end_timing - start_timing:.2f} seconds)")
    print(f"Year with the most activities: {most_activities_year}")
    print(f"Year with the most recorded hours: {most_recorded_hours_year}")
    
    # Task 2.7: Find the total distance walked in 2008 by user with id=112
    start_timing = time.time()
    user_id = "112"
    year = 2008
    total_distance = 0.0

    # Pipeline to find walking activities for user 112 in 2008
    pipeline = [
        {"$match": {"user_id": user_id, "transportation_mode": "walk"}},  # Match activities with user_id 112 and transportation_mode "walk"
        {"$project": {
            "year": {"$year": {"$dateFromString": {"dateString": "$start_date_time"}}},
            "activity_id": 1
        }},  # Project the year and activity_id
        {"$match": {"year": year}}  # Match activities in the year 2008
    ]
    activities = list(db.Activity.aggregate(pipeline))
    activity_ids = [activity["_id"] for activity in activities]

    # Pipeline to find track points for the matched activities
    track_points_pipeline = [
        {"$match": {"activity_id": {"$in": activity_ids}}},  # Match track points with the activity_ids
        {"$sort": {"activity_id": 1, "date_time": 1}},  # Sort track points by activity_id and date_time
        {"$group": {
            "_id": "$activity_id",
            "track_points": {"$push": {"lat": "$lat", "lon": "$lon"}}
        }}  # Group track points by activity_id
    ]
    track_points_groups = list(db.TrackPoint.aggregate(track_points_pipeline))

    # Calculate the total distance walked
    for group in track_points_groups:
        track_points = group["track_points"]
        for i in range(1, len(track_points)):
            point1 = (track_points[i-1]["lat"], track_points[i-1]["lon"])
            point2 = (track_points[i]["lat"], track_points[i]["lon"])
            total_distance += haversine.haversine(point1, point2)

    end_timing = time.time()
    print(f"\nTask 2.7: Find the total distance walked in 2008 by user with id=112 (Executed in {end_timing - start_timing:.2f} seconds)")
    print(f"Total distance walked in 2008 by user 112: {total_distance:.2f} km")
    
    # Task 2.8: Find the top 20 users who have gained the most altitude meters
    start_timing = time.time()
    max_duration = 300  # Maximum duration in seconds
    top_altitude_users = None

    # Aggregate altitude gain per user
    pipeline = [
        {"$lookup": {
            "from": "Activity",
            "localField": "_id",
            "foreignField": "user_id",
            "as": "activities"
        }},  # Join with the Activity collection to get activities for each user
        {"$unwind": "$activities"},  # Deconstructs the activities array field from the input documents to output a document for each element
        {"$lookup": {
            "from": "TrackPoint",
            "localField": "activities._id",
            "foreignField": "activity_id",
            "as": "track_points"
        }},  # Join with the TrackPoint collection to get track points for each activity
        {"$unwind": "$track_points"},  # Deconstructs the track_points array field from the input documents to output a document for each element
        {"$group": {
            "_id": "$_id",
            "altitudes": {"$push": "$track_points.altitude"}
        }},  # Group track points by user ID and collect altitudes
        {"$project": {
            "total_altitude_gain": {
                "$sum": {
                    "$map": {
                        "input": {"$range": [1, {"$size": "$altitudes"}]},
                        "as": "idx",
                        "in": {
                            "$cond": [
                                {"$gt": [{"$arrayElemAt": ["$altitudes", "$$idx"]}, {"$arrayElemAt": ["$altitudes", {"$subtract": ["$$idx", 1]}]}]},
                                {"$subtract": [{"$arrayElemAt": ["$altitudes", "$$idx"]}, {"$arrayElemAt": ["$altitudes", {"$subtract": ["$$idx", 1]}]}]},
                                0
                            ]
                        }
                    }
                }
            }
        }},  # Calculate the total altitude gain for each user
        {"$sort": {"total_altitude_gain": -1}},  # Sort users by total altitude gain in descending order
        {"$limit": 20}  # Limit the result to the top 20 users
    ]

    try:
        top_altitude_users = list(db.User.aggregate(pipeline, allowDiskUse=True, maxTimeMS=max_duration * 1000))
    except Exception as e:
        end_timing = time.time()
        print(f"\nTask 2.8: Find the top 20 users who have gained the most altitude meters - Timed out... (Time elapsed {end_timing - start_timing:.2f} seconds)")
    
    if top_altitude_users is not None:
        end_timing = time.time()
        print(f"\nTask 2.8: Find the top 20 users who have gained the most altitude meters (Executed in {end_timing - start_timing:.2f} seconds)")
        pprint(top_altitude_users)
    
    # Task 2.9: Find all users who have invalid activities
    # Task 2.9: Find all users who have invalid activities
    start_timing = time.time()
    max_duration = 300  # Maximum duration in seconds

    # Aggregate invalid activities per user
    pipeline = [
        {"$lookup": {
            "from": "Activity",
            "localField": "_id",
            "foreignField": "user_id",
            "as": "activities"
        }},  # Join with the Activity collection to get activities for each user
        {"$unwind": "$activities"},  # Deconstructs the activities array field from the input documents to output a document for each element
        {"$lookup": {
            "from": "TrackPoint",
            "localField": "activities._id",
            "foreignField": "activity_id",
            "as": "track_points"
        }},  # Join with the TrackPoint collection to get track points for each activity
        {"$unwind": "$track_points"},  # Deconstructs the track_points array field from the input documents to output a document for each element
        {"$sort": {"activities._id": 1, "track_points.date_time": 1}},  # Sort track points by activity_id and date_time
        {"$group": {
            "_id": "$activities._id",
            "user_id": {"$first": "$_id"},
            "track_points": {"$push": "$track_points.date_time"}
        }},  # Group track points by activity_id and collect date_times
        {"$project": {
            "user_id": 1,
            "invalid": {
                "$size": {
                    "$filter": {
                        "input": {"$range": [1, {"$size": "$track_points"}]},
                        "as": "idx",
                        "cond": {
                            "$gte": [
                                {"$subtract": [
                                    {"$arrayElemAt": ["$track_points", "$$idx"]},
                                    {"$arrayElemAt": ["$track_points", {"$subtract": ["$$idx", 1]}]}
                                ]},
                                300000  # 5 minutes in milliseconds
                            ]
                        }
                    }
                }
            }
        }},  # Calculate the number of invalid track points (time difference >= 5 minutes)
        {"$match": {"invalid": {"$gt": 0}}},  # Filter activities with invalid track points
        {"$group": {
            "_id": "$user_id",
            "num_invalid_activities": {"$sum": 1}
        }},  # Group by user ID and count the number of invalid activities per user
        {"$sort": {"num_invalid_activities": -1}}  # Sort users by the number of invalid activities in descending order
    ]

    invalid_activities_users = None
    try:
        invalid_activities_users = list(db.User.aggregate(pipeline, allowDiskUse=True, maxTimeMS=max_duration * 1000))
    except Exception as e:
        end_timing = time.time()
        print(f"\nTask 2.9: Find all users who have invalid activities and the number of invalid activities per user - Timed out... (Time elapsed {end_timing - start_timing:.2f} seconds)")

    if invalid_activities_users is not None:
        end_timing = time.time()
        print(f"\nTask 2.9: Find all users who have invalid activities and the number of invalid activities per user (Executed in {end_timing - start_timing:.2f} seconds)")
        pprint(invalid_activities_users)
    
    # Task 2.10: Find the users who have tracked an activity in the Forbidden City of Beijing
    start_timing = time.time()
    
    # Define the coordinates range for the Forbidden City
    lat_min, lat_max = 39.915, 39.917
    lon_min, lon_max = 116.396, 116.398

    # Find activity IDs with track points within the Forbidden City coordinates
    activity_ids = db.TrackPoint.distinct(
        "activity_id",
        {
            "lat": {"$gte": lat_min, "$lte": lat_max},
            "lon": {"$gte": lon_min, "$lte": lon_max}
        }
    )

    # Find user IDs associated with these activity IDs
    forbidden_city_users = db.Activity.distinct("user_id", {"_id": {"$in": activity_ids}})
    forbidden_city_users.sort()
    
    end_timing = time.time()
    print(f"\nTask 2.10: Find the users who have tracked an activity in the Forbidden City of Beijing (Executed in {end_timing - start_timing:.2f} seconds)")
    pprint(forbidden_city_users)
    
    # Task 2.11: Find all users who have registered transportation_mode and their most used transportation_mode
    start_timing = time.time()
    # Pipeline to find all users who have registered transportation_mode and their most used transportation_mode
    pipeline = [
        {"$match": {"transportation_mode": {"$ne": None}}},  # Filter activities to find those with a non-null transportation_mode
        {"$group": {"_id": {"user_id": "$user_id", "transportation_mode": "$transportation_mode"}, "count": {"$sum": 1}}},  # Group by user_id and transportation_mode, and count the number of activities for each mode
        {"$sort": {"_id.user_id": 1, "count": -1}},  # Sort the results by user_id and count in descending order
        {"$group": {"_id": "$_id.user_id", "most_used_transportation_mode": {"$first": "$_id.transportation_mode"}}},  # Group by user_id and get the most used transportation_mode
        {"$sort": {"_id": 1}}  # Sort the results by user_id in ascending order
    ]
    most_used_transportation_modes = list(db.Activity.aggregate(pipeline))
    end_timing = time.time()
    print(f"\nTask 2.11: Find all users who have registered transportation_mode and their most used transportation_mode (Executed in {end_timing - start_timing:.2f} seconds)")
    pprint(most_used_transportation_modes)
    
    connection.close_connection()

if __name__ == '__main__':
    main()
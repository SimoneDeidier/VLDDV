from DbConnector import DbConnector
from tabulate import tabulate
import os
import datetime
from haversine import haversine
import time

def main():
    # Task 1.1: connect to the DB
    db = DbConnector()
    
    print("Creating tables...")
    
    # Task 1.2: create tables
    start_timing = time.time()
    query = """CREATE TABLE IF NOT EXISTS User (
                id VARCHAR(256) NOT NULL PRIMARY KEY,
                has_labels BOOLEAN NOT NULL)
            """
    db.cursor.execute(query)
    db.db_connection.commit()
    
    query = """CREATE TABLE IF NOT EXISTS Activity (
                id INT NOT NULL PRIMARY KEY AUTO_INCREMENT,
                user_id VARCHAR(256) NOT NULL,
                transportation_mode VARCHAR(256),
                start_date_time DATETIME NOT NULL,
                end_date_time DATETIME NOT NULL,
                FOREIGN KEY (user_id) REFERENCES User(id)
                ON DELETE CASCADE ON UPDATE CASCADE)
            """
    db.cursor.execute(query)
    db.db_connection.commit()
    
    query = """CREATE TABLE IF NOT EXISTS TrackPoint (
                id INT NOT NULL PRIMARY KEY AUTO_INCREMENT,
                activity_id INT NOT NULL,
                lat DOUBLE NOT NULL,
                lon DOUBLE NOT NULL,
                altitude INT NOT NULL,
                date_days DOUBLE NOT NULL,
                date_time DATETIME NOT NULL,
                FOREIGN KEY (activity_id) REFERENCES Activity(id)
                ON DELETE CASCADE ON UPDATE CASCADE)
            """
    db.cursor.execute(query)
    db.db_connection.commit()
    end_timing = time.time()
    print(f"Tables created in {end_timing - start_timing:.2f} seconds!\nInserting data...\n\tInserting User data...")
    
    # Task 1.3: insert data
    start_timing = time.time()
    db.cursor.execute("SELECT COUNT(*) FROM User")
    user_count = db.cursor.fetchone()[0]

    if user_count == 0:
        dataset_folder = 'dataset/Data'
        labeled_ids_file = 'dataset/labeled_ids.txt'

        with open(labeled_ids_file, 'r') as file:
            labeled_ids = set(file.read().splitlines())

        for folder_name in os.listdir(dataset_folder):
            folder_path = os.path.join(dataset_folder, folder_name)
            if os.path.isdir(folder_path):
                has_labels = folder_name in labeled_ids
                query = "INSERT INTO User (id, has_labels) VALUES (%s, %s)"
                db.cursor.execute(query, (folder_name, has_labels))

        db.db_connection.commit()
    end_timing = time.time()
    print(f"\tUser data inserted in {end_timing - start_timing:.2f} seconds!\n\tInserting Activity data...")
    
    start_timing = time.time()
    def parse_date(date_str, time_str):
        try:
            return datetime.datetime.strptime(date_str + ' ' + time_str, '%Y/%m/%d %H:%M:%S')
        except ValueError:
            return datetime.datetime.strptime(date_str + ' ' + time_str, '%Y-%m-%d %H:%M:%S')

    db.cursor.execute("SELECT COUNT(*) FROM Activity")
    activity_count = db.cursor.fetchone()[0]

    if activity_count == 0:
        for user_id in os.listdir(dataset_folder):
            user_folder_path = os.path.join(dataset_folder, user_id)
            if os.path.isdir(user_folder_path):
                labels = {}
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
                                if user_id in labeled_ids:
                                    for (start_time, end_time), mode in labels.items():
                                        if start_date_time == parse_date(*start_time.split()) and end_date_time == parse_date(*end_time.split()):
                                            transportation_mode = mode
                                            break
                                query = """INSERT INTO Activity (user_id, transportation_mode, start_date_time, end_date_time)
                                           VALUES (%s, %s, %s, %s)"""
                                db.cursor.execute(query, (user_id, transportation_mode, start_date_time, end_date_time))
        
        db.db_connection.commit()
    end_timing = time.time()
    print(f"\tActivity data inserted in {end_timing - start_timing:.2f} seconds!\n\tInserting TrackPoint data...")
    
    start_timing = time.time()
    db.cursor.execute("SELECT COUNT(*) FROM TrackPoint")
    trackpoint_count = db.cursor.fetchone()[0]

    if trackpoint_count == 0:
        batch_size = 1000
        trackpoints = []

        for user_id in os.listdir(dataset_folder):
            user_folder_path = os.path.join(dataset_folder, user_id)
            if os.path.isdir(user_folder_path):
                trajectory_folder_path = os.path.join(user_folder_path, 'Trajectory')
                for plt_file_name in os.listdir(trajectory_folder_path):
                    if plt_file_name.endswith('.plt'):
                        plt_file_path = os.path.join(trajectory_folder_path, plt_file_name)
                        with open(plt_file_path, 'r') as plt_file:
                            lines = plt_file.readlines()[6:]
                            if len(lines) < 2500:
                                start_date_time = parse_date(*lines[0].strip().split(',')[5:7])
                                end_date_time = parse_date(*lines[-1].strip().split(',')[5:7])
                                
                                query = """SELECT id FROM Activity 
                                    WHERE user_id = %s AND start_date_time = %s AND end_date_time = %s"""
                                db.cursor.execute(query, (user_id, start_date_time, end_date_time))
                                activity_id = db.cursor.fetchone()
                                
                                if activity_id:
                                    activity_id = activity_id[0]
                                    for line in lines:
                                        lat, lon, _, altitude, date_days, date, time_str = line.strip().split(',')
                                        date_time = parse_date(date, time_str)
                                        altitude = int(float(altitude))
                                        trackpoints.append((activity_id, float(lat), float(lon), altitude, float(date_days), date_time))
                                        
                                        if len(trackpoints) >= batch_size:
                                            query = """INSERT INTO TrackPoint (activity_id, lat, lon, altitude, date_days, date_time)
                                                VALUES (%s, %s, %s, %s, %s, %s)"""
                                            db.cursor.executemany(query, trackpoints)
                                            db.db_connection.commit()
                                            trackpoints.clear()
        
        if trackpoints:
            query = """INSERT INTO TrackPoint (activity_id, lat, lon, altitude, date_days, date_time)
                VALUES (%s, %s, %s, %s, %s, %s)"""
            db.cursor.executemany(query, trackpoints)
            db.db_connection.commit()
            trackpoints.clear()
    end_timing = time.time()
    print(f"\tTrackPoint data inserted in {end_timing - start_timing:.2f} seconds!\nData inserted!")
    
    def display_table_data(table_name):
        query = f"SELECT * FROM {table_name} LIMIT 10"
        db.cursor.execute(query)
        rows = db.cursor.fetchall()
        if rows:
            headers = [i[0] for i in db.cursor.description]
            print(f"\nFirst 10 rows of {table_name} table:")
            print(tabulate(rows, headers, tablefmt='psql'))
        else:
            print(f"\nNo data found in {table_name} table.")

    display_table_data('User')
    display_table_data('Activity')
    display_table_data('TrackPoint')
 
    print("\n\n---------QUERIES---------\n\n")
 
    def count_rows(table_name):
        query = f"SELECT COUNT(*) FROM {table_name}"
        db.cursor.execute(query)
        return db.cursor.fetchone()[0]

    start_timing = time.time()
    user_count = count_rows('User')
    activity_count = count_rows('Activity')
    trackpoint_count = count_rows('TrackPoint')
    end_timing = time.time()
    print(f"\nTask 2.1: How many users, activities and trackpoints are there in the dataset? (Executed in {end_timing - start_timing:.2f} seconds)")
    print(f"Number of users: {user_count}")
    print(f"Number of activities: {activity_count}")
    print(f"Number of trackpoints: {trackpoint_count}")
    
    start_timing = time.time()
    query = "SELECT AVG(activity_count) FROM (SELECT COUNT(*) as activity_count FROM Activity GROUP BY user_id) as subquery"
    db.cursor.execute(query)
    average_activities_per_user = db.cursor.fetchone()[0]
    end_timing = time.time()
    print(f"\nTask 2.2: Find the average number of activities per user. (Executed in {end_timing - start_timing:.2f} seconds)")
    print(f"Average number of activities per user: {average_activities_per_user:.2f}")
    
    start_timing = time.time()
    query = """SELECT user_id, COUNT(*) as activity_count 
               FROM Activity 
               GROUP BY user_id 
               ORDER BY activity_count DESC 
               LIMIT 20"""
    db.cursor.execute(query)
    top_users = db.cursor.fetchall()
    end_timing = time.time()
    print(f"\nTask 2.3: Find the top 20 users with the highest number of activities. (Executed in {end_timing - start_timing:.2f} seconds)")
    print(tabulate(top_users, headers=["User ID", "Activity Count"], tablefmt='psql'))
    
    start_timing = time.time()
    query = """SELECT DISTINCT user_id 
               FROM Activity 
               WHERE transportation_mode = 'taxi'"""
    db.cursor.execute(query)
    taxi_users = db.cursor.fetchall()
    end_timing = time.time()
    print(f"\nTask 2.4: Find all users who have taken a taxi. (Executed in {end_timing - start_timing:.2f} seconds)")
    print(tabulate(taxi_users, headers=["User ID"], tablefmt='psql'))
    
    start_timing = time.time()
    query = """SELECT transportation_mode, COUNT(*) as activity_count 
               FROM Activity 
               WHERE transportation_mode IS NOT NULL 
               GROUP BY transportation_mode 
               ORDER BY activity_count DESC"""
    db.cursor.execute(query)
    transportation_modes = db.cursor.fetchall()
    end_timing = time.time()
    print(f"\nTask 2.5: Find all types of transportation modes and count how many activities that are tagged with these transportation mode labels. (Executed in {end_timing - start_timing:.2f} seconds)")
    print(tabulate(transportation_modes, headers=["Transportation Mode", "Activity Count"], tablefmt='psql'))
    
    start_timing = time.time()
    query = """SELECT YEAR(start_date_time) as year, COUNT(*) as activity_count 
               FROM Activity 
               GROUP BY year 
               ORDER BY activity_count DESC 
               LIMIT 1"""
    db.cursor.execute(query)
    most_activities_year = db.cursor.fetchone()
    end_timing = time.time()
    print(f"\nTask 2.6: Find the year with the most activities. Is this also the year with most recorded hours? (Executed in {end_timing - start_timing:.2f} seconds)")
    print(f"Year with the most activities: {most_activities_year[0]} with {most_activities_year[1]} activities")

    start_timing = time.time()
    query = """SELECT YEAR(start_date_time) as year, 
                      SUM(TIMESTAMPDIFF(HOUR, start_date_time, end_date_time)) as total_hours 
               FROM Activity 
               GROUP BY year 
               ORDER BY total_hours DESC 
               LIMIT 1"""
    db.cursor.execute(query)
    most_hours_year = db.cursor.fetchone()
    end_timing = time.time()
    print(f"Year with the most recorded hours: {most_hours_year[0]} with {most_hours_year[1]} hours (Executed in {end_timing - start_timing:.2f} seconds)")

    if most_activities_year[0] == most_hours_year[0]:
        print("Yes, the year with the most activities is also the year with the most recorded hours.")
    else:
        print("No, the year with the most activities is not the year with the most recorded hours.")
        
    start_timing = time.time()
    query = """SELECT T1.lat, T1.lon, T2.lat, T2.lon
               FROM TrackPoint T1 JOIN TrackPoint T2
               ON T1.activity_id = T2.activity_id AND T1.id = T2.id - 1
               JOIN Activity A
               ON T1.activity_id = A.id
               WHERE A.user_id = '112' AND A.transportation_mode = 'walk' AND YEAR(A.start_date_time) = 2008"""
    db.cursor.execute(query)
    trackpoints = db.cursor.fetchall()

    total_distance = 0.0
    for lat1, lon1, lat2, lon2 in trackpoints:
        total_distance += haversine((lat1, lon1), (lat2, lon2))
    end_timing = time.time()
    print(f"\nTask 2.7: Find the total distance (in km) walked in 2008, by user with id=112. (Executed in {end_timing - start_timing:.2f} seconds)")
    print(f"Total distance walked in 2008 by user 112: {total_distance:.2f} km")
    
    start_timing = time.time()
    query = """SELECT A.user_id, SUM(T2.altitude - T1.altitude) as total_altitude_gain
               FROM TrackPoint T1 
               JOIN TrackPoint T2 ON T1.activity_id = T2.activity_id AND T1.id = T2.id - 1
               JOIN Activity A ON T1.activity_id = A.id
               WHERE T2.altitude > T1.altitude AND T1.altitude != -777 AND T2.altitude != -777
               GROUP BY A.user_id
               ORDER BY total_altitude_gain DESC
               LIMIT 20"""
    db.cursor.execute(query)
    top_altitude_users = db.cursor.fetchall()
    end_timing = time.time()
    top_altitude_users = [(user_id, total_altitude_gain / 1000) for user_id, total_altitude_gain in top_altitude_users]
    print(f"\nTask 2.8: Find the top 20 users who have gained the most altitude meters. (Executed in {end_timing - start_timing:.2f} seconds)")
    print(tabulate(top_altitude_users, headers=["User ID", "Total Altitude Gain (kilometers)"], tablefmt='psql'))
    
    start_timing = time.time()
    query = """SELECT A.user_id, COUNT(DISTINCT A.id) as invalid_activity_count
               FROM TrackPoint T1
               JOIN TrackPoint T2 ON T1.activity_id = T2.activity_id AND T1.id = T2.id - 1
               JOIN Activity A ON T1.activity_id = A.id
               WHERE TIMESTAMPDIFF(MINUTE, T1.date_time, T2.date_time) >= 5
               GROUP BY A.user_id
               HAVING invalid_activity_count > 0
               ORDER BY invalid_activity_count DESC"""
    db.cursor.execute(query)
    invalid_activities = db.cursor.fetchall()
    end_timing = time.time()
    print(f"\nTask 2.9: Find all users who have invalid activities. (Executed in {end_timing - start_timing:.2f} seconds)")
    print(tabulate(invalid_activities, headers=["User ID", "Invalid Activity Count"], tablefmt='psql'))
    
    start_timing = time.time()
    query = """SELECT DISTINCT A.user_id
               FROM TrackPoint T
               JOIN Activity A ON T.activity_id = A.id
               WHERE T.lat BETWEEN 39.915 AND 39.917
               AND T.lon BETWEEN 116.396 AND 116.398"""
    db.cursor.execute(query)
    forbidden_city_users = db.cursor.fetchall()
    end_timing = time.time()
    print(f"\nTask 2.10: Find the users who have tracked an activity in the Forbidden City of Beijing. (Executed in {end_timing - start_timing:.2f} seconds)")
    print(tabulate(forbidden_city_users, headers=["User ID"], tablefmt='psql'))
    
    start_timing = time.time()
    query = """SELECT user_id, transportation_mode
               FROM (
                   SELECT user_id, transportation_mode, 
                          ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY COUNT(*) DESC) as rn
                   FROM Activity
                   WHERE transportation_mode IS NOT NULL
                   GROUP BY user_id, transportation_mode
               ) as subquery
               WHERE rn = 1
               ORDER BY user_id"""
    db.cursor.execute(query)
    most_used_transportation_modes = db.cursor.fetchall()
    end_timing = time.time()
    print(f"\nTask 2.11: Find all users who have registered transportation_mode and their most used transportation_mode. (Executed in {end_timing - start_timing:.2f} seconds)")
    print(tabulate(most_used_transportation_modes, headers=["User ID", "Most Used Transportation Mode"], tablefmt='psql'))
    
    db.close_connection()
    print("Connection closed")
    
if __name__ == '__main__':
    main()

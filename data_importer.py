import csv
import mysql.connector


def timezone_datareader(path):
    """
    Read timezone data from a CSV file and insert it into the 'timezones' table in the database.

    Args:
        path (str): The path to the CSV file containing timezone data.
    """
    timezone_fail_count=0
    conn = mysql.connector.connect(
        host = 'localhost',
        user = "****",
        password = "****",
        database='loopdb')
    conn.autocommit = True
    with open(path) as timezone_csvfile:

        csv_reader = csv.reader(timezone_csvfile, delimiter = ',')
        timezone_line_count = 0
        for row in csv_reader:
            if timezone_line_count==0:
                timezone_line_count+=1
                continue
            else:
                try:

                    
                    query = f'''INSERT INTO timezones 
                    (storeid, timezone) 
                    VALUES 
                    ('{row[0]}', 
                     '{row[1]}');'''
                                            
                    cursor=conn.cursor()
                    cursor.execute(query)
                    

                    timezone_line_count+=1
                    if timezone_line_count%500==0:
                        print(timezone_line_count)
                    
                except:
                    timezone_fail_count+=1
        conn.close()
        print("Exiting timezone")

def business_hours_datareader(path):
    """
    Read business hours data from a CSV file and insert it into the 'business_hours' table in the database.

    Args:
        path (str): The path to the CSV file containing business hours data.
    """
    business_fail_count=0
    conn = mysql.connector.connect(
        host = 'localhost',
        user = "****",
        password = "****",
        database='loopdb')
    conn.autocommit = True
    with open(path) as business_hours_csvfile:
        business_csv_reader = csv.reader(business_hours_csvfile, delimiter = ',')
        business_line_count = 0
        for row in business_csv_reader:
            if business_line_count==0:
                business_line_count+=1
                continue
            else:
                try:

                    
                    query = f'''INSERT INTO `loopdb`.`business_hours`
                    (`store_id`,
                    `day`,
                    `start_time`,
                    `end_time`)
                    VALUES
                    ('{row[0]}',
                    '{row[1]}',
                    '{row[2]}',
                    '{row[3]}');'''
                    
                    cursor=conn.cursor()
                    cursor.execute(query)
                    business_line_count+=1
                    if business_line_count%500==0:
                        print(business_line_count)
                except:
                    business_fail_count+=1
        conn.close()
        print("Exiting business hours")

def store_status_datareader(path):
    """
    Read store status data from a CSV file and insert it into the 'store_status' table in the database.

    Args:
        path (str): The path to the CSV file containing store status data.
    """
    status_fail_count=0
    conn = mysql.connector.connect(
        host = 'localhost',
        user = "****",
        password = "****",
        database='loopdb')
    conn.autocommit = True
    with open(path) as store_status_csvfile:

        csv_reader = csv.reader(store_status_csvfile, delimiter = ',')
        status_line_count = 0
        for row in csv_reader:
            if status_line_count==0:
                status_line_count+=1
                continue
            else:
                try:
                    
                    
                    timestamp_split = row[2].split()
                    final_timestamp = timestamp_split[0] +' '+ timestamp_split[1]
                    
                    query = f'''INSERT INTO `loopdb`.`store_status`
                    (`store_id`,
                    `timestamp`,
                    `status`)
                    VALUES
                    ('{row[0]}',
                    '{final_timestamp}',
                    '{row[1]}');'''
                                            
                    cursor=conn.cursor()
                    cursor.execute(query)
                    

                    status_line_count+=1
                    if status_line_count%500==0:
                        print(status_line_count)
                    
                except:
                    status_fail_count+=1
        conn.close()
        print("Exiting status")

timezone_path = 'C:\\Users\\Rishabh\\Documents\\TakeHomeLoop\\timezones.csv'
business_hours_path = 'C:\\Users\\Rishabh\\Documents\\TakeHomeLoop\\Menu_hours.csv'
store_status_path = 'C:\\Users\\Rishabh\\Documents\\TakeHomeLoop\\store_status.csv'

timezone_datareader(timezone_path)
business_hours_datareader(business_hours_path)
store_status_datareader(store_status_path)
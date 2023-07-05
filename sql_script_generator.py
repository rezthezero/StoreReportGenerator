import csv
import mysql.connector



def timezone_datareader(path):
    """
    Read timezone data from a CSV file and generate SQL queries to insert the data into the 'timezones' table.
    
    Args:
        path (str): The path to the CSV file containing timezone data.
    """
    with open(path) as timezone_csvfile:
        with open("timezone.sql", "w") as timezone_sqlfile:
            csv_reader = csv.reader(timezone_csvfile, delimiter = ',')
            first_line = True
            for row in csv_reader:
                if first_line:
                    first_line = False
                    continue
                query = f'''INSERT INTO `loopdb`.`timezones` 
                (`storeid`, `timezone`)
                VALUES
                ('{row[0]}', '{row[1]}');
                '''
                timezone_sqlfile.write(query)

def business_hours_datareader(path):
    """
    Read business hours data from a CSV file and generate SQL queries to insert the data into the 'business_hours' table.
    
    Args:
        path (str): The path to the CSV file containing business hours data.
    """
    conn = mysql.connector.connect(
        host = 'localhost',
        user = "****",
        password = "****",
        database='loopdb')
    conn.autocommit = True
    
    with open(path) as business_hours_csvfile:
        with open("business.sql", "w") as business_sqlfile:
            business_csv_reader = csv.reader(business_hours_csvfile, delimiter = ',')
            first_line = True
            for row in business_csv_reader:
                if first_line:
                    first_line = False
                    continue
                query = f'''INSERT INTO `loopdb`.`business_hours`
                (`store_id`,`day`,`start_time`,`end_time`)
                VALUES
                ('{row[0]}','{row[1]}','{row[2]}','{row[3]}');
                '''
                business_sqlfile.write(query)
        

def store_status_datareader(path):
    """
    Read store status data from a CSV file and generate SQL queries to insert the data into the 'store_status' table.
    
    Args:
        path (str): The path to the CSV file containing store status data.
    """
    conn = mysql.connector.connect(
        host = 'localhost',
        user = "****",
        password = "****",
        database='loopdb')
    conn.autocommit = True
    
    with open(path) as store_status_csvfile:
        with open("status.sql", "w") as status_sqlfile:
            csv_reader = csv.reader(store_status_csvfile, delimiter = ',')
            first_line = True
            for row in csv_reader:
                
                if first_line:
                    first_line = False
                    continue
                timestamp_split = row[2].split()
                final_timestamp = timestamp_split[0] +' '+ timestamp_split[1]
                
                query = f'''INSERT INTO `loopdb`.`store_status`
                (`store_id`,`timestamp`,`status`)
                VALUES
                ('{row[0]}','{final_timestamp}','{row[1]}');
                '''
                status_sqlfile.write(query)


timezone_path = 'C:\\Users\\Rishabh\\Documents\\TakeHomeLoop\\timezones.csv'
business_hours_path = 'C:\\Users\\Rishabh\\Documents\\TakeHomeLoop\\Menu_hours.csv'
store_status_path = 'C:\\Users\\Rishabh\\Documents\\TakeHomeLoop\\store_status.csv'

timezone_datareader(timezone_path)
business_hours_datareader(business_hours_path)
store_status_datareader(store_status_path)
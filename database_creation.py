import mysql.connector
import logging

logging.getLogger().setLevel(logging.INFO)

def drop_all():
    """
    Drop all existing tables from the database.
    """
    conn = mysql.connector.connect(
        host = 'localhost',
        user = "****",
        password = '****',
        database='loopdb')
    
    query = '''DROP TABLE IF EXISTS timezones;
    DROP TABLE IF EXISTS store_status;
    DROP TABLE IF EXISTS business_hours;'''
    
    cursor = conn.cursor()
    cursor.execute(query, multi=True)

def create_timezones_table():
    """
    Create the 'timezones' table in the database.
    """
    conn = mysql.connector.connect(
        host = 'localhost',
        user = "****",
        password = "****",
        database='loopdb')
    
    try:
        drop_query = '''DROP TABLE IF EXISTS timezones;'''
        create_query = '''CREATE TABLE `loopdb`.`timezones` (
        `storeid` varchar(32) NOT NULL,
        `timezone` varchar(64) NOT NULL,
        PRIMARY KEY (`storeid`),
        UNIQUE KEY `storeid_UNIQUE` (`storeid`));
        '''
        
        cursor = conn.cursor()
        cursor.execute(drop_query)
        cursor.execute(create_query)
        logging.info("Table 'timezones' created successfully.")
    
    except:
        logging.error("Table 'timezones' unable to be created")
    
    finally:
        conn.close()
    
def create_store_status_table():
    """
    Create the 'store_status' table in the database.
    """
    conn = mysql.connector.connect(
        host = 'localhost',
        user = "****",
        password = "****",
        database='loopdb')
    
    try:
        drop_query = '''DROP TABLE IF EXISTS store_status;'''
        create_query = '''CREATE TABLE `loopdb`.`store_status` (
        `store_id` VARCHAR(32) NOT NULL,
        `timestamp` DATETIME NOT NULL,
        `status` VARCHAR(45) NOT NULL);
        '''
        
        cursor = conn.cursor()
        cursor.execute(drop_query)
        cursor.execute(create_query)
        logging.info("Table 'store_status' created successfully.")
        
    except:
        logging.error("Table 'store_status' unable to be created")
    
    finally:
        conn.close()

def create_business_hours_table():
    """
    Create the 'business_hours' table in the database.
    """
    conn = mysql.connector.connect(
        host = 'localhost',
        user = "****",
        password = "****",
        database='loopdb')
    
    try:
        drop_query = '''DROP TABLE IF EXISTS business_hours;'''
        create_query = '''CREATE TABLE `loopdb`.`business_hours` (
        `store_id` VARCHAR(32) NOT NULL,
        `day` INT NOT NULL,
        `start_time` TIME NOT NULL,
        `end_time` TIME NOT NULL);
        '''
        
        cursor = conn.cursor()
        cursor.execute(drop_query)
        cursor.execute(create_query)
        logging.info("Table 'business_hours' created successfully.")
        
    except:
        logging.error("Table 'business_hours' unable to be created")
    
    finally:
        conn.close()

# Create the new table
# drop_all()
create_timezones_table()
create_store_status_table()
create_business_hours_table()


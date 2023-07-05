import mysql.connector
import datetime
import pytz
from flask import Flask
from flask import send_file
import uuid
import threading

#Initiating a connection to the database. [The user, password and database details will have to be replaced with the suitable values]
conn = mysql.connector.connect(
    host = 'localhost',
    user = "root",
    password = "Pra-vin1",
    database='loopdb')
conn.autocommit = True

report_status={}
def get_store_data(store_id, current_timestamp):
    """
    Retrieve the store status data for the last week for the given store ID and the current timestamp.

    Args:
        store_id (str): The ID of the store.
        current_timestamp (str): The current timestamp in the format "YYYY-MM-DD HH:MM:SS".

    Returns:
        list: A list of tuples containing store ID, timestamp, and status.
    """
    current_time = datetime.datetime.strptime(current_timestamp, "%Y-%m-%d %H:%M:%S")
    pastweek_time = current_time - datetime.timedelta(days=7)
    
    lastweek_query = f'''select store_id, timestamp, status from store_status 
                    where 
                    timestamp >= '{pastweek_time}' and timestamp <= '{current_timestamp}'
                    and store_id = '{store_id}'
                    order by store_id asc, timestamp asc;
                    '''
    
    cursor = conn.cursor()
    cursor.execute(lastweek_query)
    polling_list = cursor.fetchall()
    return polling_list    

def get_store_tz(store_id):
    """
    Retrieve the timezone for the given store ID from the timezones table in the database.

    Args:
        store_id (str): The ID of the store.

    Returns:
        str: The timezone of the store.
    """
    tz_query = f'''select timezone from timezones where storeid = '{store_id}';
                '''
    cursor = conn.cursor()
    cursor.execute(tz_query)
    tz_output = cursor.fetchall()
    if not tz_output:
        final_tz_output='America/Chicago'
        return final_tz_output
    
    final_tz_output=tz_output[0][0]
    return final_tz_output

def get_store_hours(store_id):
    """
    Retrieve the business hours for the given store ID.

    Args:
        store_id (str): The ID of the store.

    Returns:
        list: A list of tuples containing store ID, day, start time, and end time.
    """
    
    bh_query = f'''select store_id, day, start_time, end_time from business_hours
                where store_id = {store_id};
                '''
    
    cursor = conn.cursor()
    cursor.execute(bh_query)
    bh_list = cursor.fetchall()
    
    return bh_list

def utc_to_localtz(store_tz,timestamp):
    """
    Convert the UTC timestamp to the local timezone of the store.

    Args:
        store_tz (str): The timezone of the store.
        timestamp (datetime.datetime): The UTC timestamp.

    Returns:
        datetime.datetime: The timestamp in the local timezone of the store.
    """
    
    utc_timezone = pytz.timezone('UTC')
    local_timezone = pytz.timezone(store_tz)
    
    utc_tz_timestamp = utc_timezone.localize(timestamp) #Making the timestamp timezone-aware from timezone-naive
    return utc_tz_timestamp.astimezone(local_timezone) #Converting it into the local timezone and returning it

def get_last_week_store_hours(store_tz, bh_list, local_tz_current_timestamp):
    """
    Obtain the store hours for the last week in the local timezone.

    Args:
        store_tz (str): The timezone of the store.
        bh_list (list): A list of tuples containing store ID, day, start time, and end time.
        local_tz_current_timestamp (datetime.datetime): The current timestamp in the local timezone.

    Returns:
        dict: A dictionary containing store hours for each day of the week.
    """
    week_dict = dict()
    
    #List for having a reference to match the numbers found in the Menu_hours CSV file to specific days
    weekday_list = [(0,'Monday'),(1,'Tuesday'),(2,'Wednesday'),(3,'Thursday'),(4,'Friday'),(5,'Saturday'),(6,'Sunday')]
    
    local_timezone = pytz.timezone(store_tz)
    
    #Adding the days and dates up to the first day of the last week into the week_dict dictionary
    for i in range(0,8):
        temp_timestamp = local_tz_current_timestamp - datetime.timedelta(days=i) #Iteratively decreasing the date by a day up to 7 days.
        temp_day = temp_timestamp.strftime('%A') #Obtaining the day related to the new date
    
        if temp_day not in week_dict:
            week_dict[temp_day] = list()
        
        temp_dict_initial = {'date':temp_timestamp,'times':[]}
        week_dict[temp_day].append(temp_dict_initial)
    
    
    #Insert times into the week_dict.
    for i in bh_list:
        
        #Assigning the day based on the number present in the CSV file.
        day = ''
        for j in weekday_list:
            if i[1]==j[0]:
                day=j[1]
        
        #Adding the start and end times into week_dict.
        for j in week_dict[day]:
            day_date = j['date']
            temp_dict=dict()
            
            #Obtaining the date with the hours, minutes, seconds and microseconds set to 0
            base_date = day_date.replace(hour=0, minute=0, second=0, microsecond=0) 
            
            #Converting the start_time and end_time timedelta objects into timezone-aware datetime objects.
            final_start_time = base_date + i[2]
            final_start_time = final_start_time.astimezone(local_timezone)
            final_end_time = base_date + i[3]
            final_end_time = final_end_time.astimezone(local_timezone)    
            
            temp_dict['start_time'] = final_start_time
            temp_dict['end_time'] = final_end_time
            temp_dict['status'] = []
        
            j['times'].append(temp_dict)
    
    #The below if condition is executed when the data for the store is not found, hence making the business hours be 24/7 hours.
    if not bh_list:
        for k in week_dict.keys():
            for date_data in week_dict[k]:
                base_date = date_data['date'].replace(hour=0, minute=0, second=0, microsecond=0)
                start_time = datetime.timedelta(hours=0,minutes=0,seconds=0) + base_date
                end_time = datetime.timedelta(hours=23,minutes=59,seconds=59) + base_date
                date_data['times'].append({'start_time':start_time,'end_time':end_time,'status':[]})
    
    return week_dict
    
def map_polldata_to_store_hours(week_dict, store_tz, polling_list):
    """
    Map poll data to store hours in the dictionary with the details of the last week working hours.
    Any poll data outside of business hours is ignore.

    Args:
        week_dict (dict): A dictionary containing store hours for each day of the week.
        store_tz (str): The timezone of the store.
        polling_list (list): A list of tuples containing store ID, timestamp, and status.
    """
    
    #Iterating over the polling_data to ensure only polls within the business hours are considered.
    for i in polling_list:
        
        #Converting the UTC poll times into the local timezone of the store to allow for comparison with the store hours.
        poll_timestamp = str(i[1])
        poll_time = datetime.datetime.strptime(poll_timestamp, "%Y-%m-%d %H:%M:%S")   
        local_tz_poll_timestamp = utc_to_localtz(store_tz,poll_time)
        local_poll_day = local_tz_poll_timestamp.strftime('%A')
        
        for j in week_dict[local_poll_day]:
            for k in j['times']:
                if local_tz_poll_timestamp > k['start_time'] and local_tz_poll_timestamp < k['end_time']:
                    k['status'].append((local_tz_poll_timestamp,i[2]))
                    break
        
def get_active_count(week_dict):
    """
    Calculate the active and inactive polls for each time slot in the week dictionary.

    Args:
        week_dict (dict): A dictionary containing store hours for each day of the week.
    """
    total_active_count = 0
    total_inactive_count = 0
    
    #Iterating through each set of store hours to count how many polls have the status as either active or inactive.
    for day in week_dict.keys():
        for day_element in week_dict[day]:
            for times_element in day_element['times']:
                
                active_count = 0
                inactive_count = 0
                    
                for status_tuple_index  in range(len(times_element['status'])):
                    
                    if times_element['status'][status_tuple_index][1]=='active':
                        active_count+=1
                        total_active_count+=1
                    else:
                        inactive_count+=1
                        total_inactive_count+=1
                
                
                times_element['active_count'] = active_count
                times_element['inactive_count'] = inactive_count
                
            
    #If no polls are found within any of the business hours then the store is considered to be active by default
    #Hence the total_active_count is set to 1
    if total_active_count==0 and total_inactive_count==0:
        total_active_count=1
    
    for day in week_dict.keys():
        for day_element in week_dict[day]:
            for times_element in day_element['times']:
                
                #If there is no poll within a particular set of business hours then 
                #the active_count and inactive_count are set to the respective total counts
                #Essentially this assigns the active hours percentage to average active hour over the week
                if times_element['active_count']==0 and times_element['inactive_count']==0:
                    times_element['active_count'] = total_active_count
                    times_element['inactive_count'] = total_inactive_count
                    
                

def get_uptime_downtime(local_tz_current_timestamp, week_dict,store_tz):
    """
    Calculate uptime and downtime for the last hour, last day, and last week.

    Args:
        local_tz_current_timestamp (datetime.datetime): The current timestamp in the local timezone.
        week_dict (dict): A dictionary containing store hours for each day of the week.
        store_tz (str): The timezone of the store.

    Returns:
        tuple: A tuple containing the uptime and downtime durations for the last hour, last day, and last week.
    """
    
    #Setting the start times for the last hour, last day and last week
    last_hour_start_time = local_tz_current_timestamp - datetime.timedelta(hours=1)
    last_day_start_time = local_tz_current_timestamp - datetime.timedelta(days=1)
    last_week_start_time = local_tz_current_timestamp - datetime.timedelta(days=7)


    #Initializing variables
    last_hour_uptime = 0
    last_hour_downtime = 0
    last_day_uptime = 0
    last_day_downtime = 0
    last_week_uptime = 0
    last_week_downtime = 0
    
    #Iterating through week_dict to obtain the active and inactive sections of the timeframes (last hour, day and week)
    for day in week_dict.keys():
        for day_element in week_dict[day]:
            for times_element in day_element['times']:
                if last_hour_start_time < times_element['end_time'] and local_tz_current_timestamp > times_element['start_time']:
                    
                    #The overlap between the timeframes and the store hours is calculated
                    overlap_duration = (min(local_tz_current_timestamp,times_element['end_time'])-max(times_element['start_time'],last_hour_start_time)).total_seconds()/60
                    
                    #Overlap duration is multiplied by the fractions of active/inactive counts to obtain uptime/downtime respectively
                    last_hour_uptime += overlap_duration * (times_element['active_count']/(times_element['active_count'] + times_element['inactive_count']))
                    last_hour_downtime += overlap_duration * (times_element['inactive_count']/(times_element['active_count'] + times_element['inactive_count']))
                    
                if last_day_start_time < times_element['end_time'] and local_tz_current_timestamp > times_element['start_time']:
                    
                    overlap_duration = (min(local_tz_current_timestamp,times_element['end_time'])-max(times_element['start_time'],last_day_start_time)).total_seconds()/3600
                    
                    last_day_uptime += overlap_duration * (times_element['active_count']/(times_element['active_count'] + times_element['inactive_count']))
                    last_day_downtime += overlap_duration * (times_element['inactive_count']/(times_element['active_count'] + times_element['inactive_count']))
                    
                if last_week_start_time < times_element['end_time'] and local_tz_current_timestamp > times_element['start_time']:
                    
                    overlap_duration = (min(local_tz_current_timestamp,times_element['end_time'])-max(times_element['start_time'],last_week_start_time)).total_seconds()/3600
                    
                    last_week_uptime += overlap_duration * (times_element['active_count']/(times_element['active_count'] + times_element['inactive_count']))
                    last_week_downtime += overlap_duration * (times_element['inactive_count']/(times_element['active_count'] + times_element['inactive_count']))
    
    return last_hour_uptime,last_hour_downtime,last_day_uptime,last_day_downtime,last_week_uptime,last_week_downtime

def generate_store_report(store_id,csvfile):
    """
    Generate a report for a specific store and write the results to a CSV file.

    Args:
        store_id (str): The ID of the store.
        csvfile (_io.TextIOWrapper): The CSV file to write the report to.

    Returns:
        tuple: A tuple containing the week dictionary, store hours list, and polling list.
    """
    
    #Hardcoding the current timestamp by obtaining the latest date among the polling data
    current_timestamp = '2023-01-25 18:13:22'
    current_time = datetime.datetime.strptime(current_timestamp, "%Y-%m-%d %H:%M:%S") 
    
    #Get store data from database
    polling_list = get_store_data(store_id, current_timestamp) #Poll data containing store_id, status, timestamp
    store_tz = get_store_tz(store_id) #Timezone data containg store_id, timezone
    bh_list = get_store_hours(store_id) #Store hours data containg store_id, day, start_time, end_time
    
    local_tz_current_timestamp = utc_to_localtz(store_tz,current_time) #The current timestamp converted to the store's local timezone
    week_dict = get_last_week_store_hours(store_tz, bh_list, local_tz_current_timestamp) #Dictionary containing details regarding the last week from the current timestamp
                                                                                         # current_timestamp - 7 days
    map_polldata_to_store_hours(week_dict, store_tz, polling_list)
    
    get_active_count(week_dict)
    
    #Obtaining the uptime and downtime data.
    last_hour_uptime,last_hour_downtime,last_day_uptime,last_day_downtime,last_week_uptime,last_week_downtime = get_uptime_downtime(local_tz_current_timestamp, week_dict,store_tz)
    
    #Writing the uptime and downtime data into the CSV file passed as a parameter.
    csvfile.write(f"{store_id},{last_hour_uptime:.2f},{last_day_uptime:.2f},{last_week_uptime:.2f},{last_hour_downtime:.2f},{last_day_downtime:.2f},{last_week_downtime:.2f}\n")
    return week_dict,bh_list,polling_list


def generate_report(report_id):
    """
    Generate reports for all stores and write the results to a CSV file.

    Args:
        report_id (str): The unique id of the report generated.
    """
    
    report_status[report_id]="RUNNING"
    
    with open(f'{report_id}.csv','w') as csvfile:
        csvfile.write('STORE_ID,HOUR_UPTIME,DAY_UPTIME,WEEK_UPTIME,HOUR_DOWNTIME,DAY_DOWNTIME,WEEK_DOWNTIME\n')
        store_id_query = '''SELECT DISTINCT(`store_status`.`store_id`)
                        FROM `loopdb`.`store_status`;
                        '''
        
        cursor = conn.cursor()
        cursor.execute(store_id_query)
        store_id_list = cursor.fetchall()
        
        store_count = 0
        for i in store_id_list:
            store_count+=1
            generate_store_report(i[0], csvfile)
            
    report_status[report_id]="COMPLETE"
    

    
app = Flask(__name__)

@app.route('/trigger_report', methods=['GET'])
def trigger():
    """
    Generated a unique ID for the report and initiates the report generation.

    Returns
    -------
    uuid_str (str): The unique ID of the generated report.

    """
    uuid_str = str(uuid.uuid4()) #Generates a random unique ID to be used as the report ID
    
    #Starting a thread to initiate the report generation
    x = threading.Thread(target=generate_report, args=(uuid_str,)) 
    x.start()
    
    return uuid_str

@app.route('/get_report/<report_id>', methods=['GET'])
def get(report_id):
    """
    Checks the completion status of the report generation and returns either a string or a file depending on the status.

    Parameters
    ----------
    report_id (str): The unique ID of the generated report.

    Returns
    -------
    str: "RUNNING" if the report generation is still ongoing.
    OR
    csvfile: The final CSV file of the report is returned as an attachment.

    """
    if report_status[report_id]=="RUNNING":
        return "RUNNING"
    elif report_status[report_id]=="COMPLETE":
        return send_file(f'{report_id}.csv',mimetype='text/csv',download_name=f'{report_id}.csv',as_attachment=True)
    else:
        return "Report ID not found!"

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=8080)



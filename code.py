import json, requests
import sqlite3
from datetime import datetime
import matplotlib.pyplot as plt
import numpy as np

#Weather API
def weather_conditions(query):
    '''Input: query to select location to search on weather API
    Function: calls to API and collects weather data for given query
    Returns: success statement re. adding data into database
    '''
    YourWeatherAppID = '43f85097de0ee762c30b7f1f558a2fcd'
    query = query
    weather_url = "http://api.openweathermap.org/data/2.5/weather?q=" + query + "&units=imperial&APPID=" +YourWeatherAppID
    weather_response = requests.get(weather_url).json()
    datetime = weather_response['dt']
    main = weather_response['weather'][0]['main']
    description = weather_response['weather'][0]['description']
    conn = sqlite3.connect('database.db')
    cur = conn.cursor()
    try:
        cur.execute("CREATE TABLE Weather (query TEXT, datetime REAL, main TEXT, description TEXT)")
    except:
        print("Table already exists")
    cur.execute("INSERT INTO Weather (query, datetime, main, description) VALUES (?,?,?,?)",(query,datetime,main,description))
    #cur.execute("INSERT IGNORE INTO Weather (query, datetime, main, description) VALUES (?,?,?,?)",(query,datetime,main,description))
    conn.commit()
    return "Successfully entered " + str(datetime) + main + description + " into database"
    

def min_max_temp(query):
    '''Input: query to select location to search on weather API
    Function: calls to API and collects temperature data for given query
    Returns: success statement re. adding data into database
    '''
    YourWeatherAppID = '43f85097de0ee762c30b7f1f558a2fcd'
    query = query
    weather_url = "http://api.openweathermap.org/data/2.5/weather?q=" + query + "&units=imperial&APPID=" +YourWeatherAppID
    weather_response = requests.get(weather_url).json()
    datetime = weather_response['dt']
    temp = weather_response['main']['temp']
    max_temp = weather_response['main']['temp_max']
    min_temp = weather_response['main']['temp_min']
    conn = sqlite3.connect('database.db')
    cur = conn.cursor()
    try:
        cur.execute("CREATE TABLE Temperature (query TEXT, datetime REAL, temp REAL, max_temp REAL, min_temp REAL)")
    except:
        print("Table already exists")
    cur.execute("INSERT INTO Temperature (query, datetime, temp, max_temp, min_temp) VALUES (?,?,?,?, ?)",(query,datetime,temp, max_temp, min_temp))
    conn.commit()
    return "Successfully entered " + str(datetime) + str(temp) + str(max_temp) + str(min_temp) + " into database"

#uncomment lines below to collect weather data
# print(weather_conditions('Ann+Arbor'))
# print(min_max_temp('Ann+Arbor'))

#Traffic API
# http(s)://baseURL/traffic/services/versionNumber/flowSegmentData/style/zoom/format?key=Your_API_Key&point=point&unit=unit&thickness=thickness&openLr=boolean&jsonp=jsonp
# [0] Washtenaw Ave, [1] Packard & Hill, [2] W. Stadium Blvd, [3] Saline Rd., [4] N. Maple Rd
coordinates = [(52.41072,4.84239), (42.271883, -83.741668), (42.264093, -83.765552), (42.235923, -83.775222), (42.293234, -83.780778)]

def traffic_Data(coord_lst):
    '''Input: list of coordinates to select locations to search for on traffic data API
    Function: calls to API and collects traffic data for given coordinates
    Returns: success statement re. adding data into database
    '''
    APIKEY = 'OALIuAkA3VQ5zNO2jXBBXcSVypYLHKGV'
    coordinates = coord_lst
    conn = sqlite3.connect('database.db')
    cur = conn.cursor()
    for tup in coordinates:
        coord1 = tup[0]
        coord2 = tup[-1]
        traffic_url = 'https://api.tomtom.com/traffic/services/4/flowSegmentData/absolute/10/json?key=' + APIKEY + '&point=' + str(coord1) + ',' + str(coord2)
        response = requests.get(traffic_url).json()
        print(response)
        date_time = datetime.now()
        coords = str(coord1) + ',' + str(coord2)
        travel_time = speed = response['flowSegmentData']['currentTravelTime']
        speed = response['flowSegmentData']['currentSpeed']
        if response['flowSegmentData']['roadClosure'] == True:
            road_close = 1
        else:
            road_close = 0
        try:
            cur.execute("CREATE TABLE TrafficFlow (coordinates TEXT, datetime REAL, speed INT, travel_time INT, road_closure INT)")
        except:
            print("Table already exists")
        cur.execute("INSERT INTO TrafficFlow (coordinates, datetime, speed, travel_time, road_closure) VALUES (?,?,?,?,?)",(coords,date_time, speed, travel_time, road_close))
        #cur.execute("INSERT IGNORE INTO Weather (query, datetime, main, description) VALUES (?,?,?,?)",(query,datetime,main,description))
        print("Successfully entered " + str(date_time) + str(speed) + str(travel_time) + str(road_close) + " into database")
        conn.commit()


def report_confidence(coord_lst):
    '''Input: list of coordinates to select locations to search for on traffic data API
    Function: calls to API and collects data re. confidence of traffic reports for given coordinates
    Returns: success statement re. adding data into database
    '''
    APIKEY = 'OALIuAkA3VQ5zNO2jXBBXcSVypYLHKGV'
    coordinates = coord_lst
    conn = sqlite3.connect('database.db')
    cur = conn.cursor()
    for tup in coordinates:
        coord1 = tup[0]
        coord2 = tup[-1]
        traffic_url = 'https://api.tomtom.com/traffic/services/4/flowSegmentData/absolute/10/json?key=' + APIKEY + '&point=' + str(coord1) + ',' + str(coord2)
        response = requests.get(traffic_url).json()
        print(response)
        date_time = datetime.now()
        coords = str(coord1) + ',' + str(coord2)
        confidence = response['flowSegmentData']['confidence']
        try:
            cur.execute("CREATE TABLE Confidence (coordinates TEXT, datetime REAL, confidence REAL)")
        except:
            print("Table already exists")
        cur.execute("INSERT INTO Confidence (coordinates, datetime, confidence) VALUES (?,?,?)",(coords,date_time, confidence))
        print("Successfully entered " + str(date_time) +  str(confidence) + " into database")
        conn.commit()
    return None

#uncomment to collect traffic data
# print(traffic_Data(coordinates))
# print(report_confidence(coordinates))

def clean_data():
    '''Input: None
    Function: cleans data by converting timestamps to uniform format
    Returns: None
    '''
    conn = sqlite3.connect('database.db')
    cur = conn.cursor()
    try:
        cur.execute("CREATE TABLE temp_clean (query TEXT, datetime REAL, temp REAL, max_temp REAL, min_temp REAL)")
    except:
        print("Table exists.")
    cur.execute("SELECT * From Temperature")
    data = cur.fetchall()
    for row in data:
        query = row[0]
        timestamp = row[1]
        temp = row[2]
        max_temp = row[3]
        min_temp = row[-1]
        dt_object = datetime.fromtimestamp(timestamp)
        year_month = str(dt_object)[:10]
        cur.execute("INSERT INTO temp_clean (query, datetime, temp, max_temp, min_temp) VALUES (?,?,?,?,?)", (query, year_month, temp, max_temp, min_temp))
        conn.commit()
    
    try:
        cur.execute("CREATE TABLE traffic_clean (coordinates TEXT, datetime REAL, speed INT, travel_time INT, road_closure INT)")
    except:
        print("Table exists.")
    cur.execute("SELECT * From TrafficFlow")
    data = cur.fetchall()
    for row in data:
        coordinates = row[0]
        timestamp = str(row[1])[:10]
        speed = row[2]
        travel_time = row[3]
        closure = row[-1]
        cur.execute("INSERT INTO traffic_clean (coordinates, datetime, speed, travel_time, road_closure) VALUES (?,?,?,?,?)", (coordinates, timestamp, speed, travel_time, closure))
        conn.commit()
    
    try:
        cur.execute("CREATE TABLE confidence_clean (coordinates TEXT, datetime REAL, confidence REAL)")
    except:
        print("Table exists.")
    cur.execute("SELECT * From Confidence")
    data = cur.fetchall()
    for row in data:
        coordinates = row[0]
        timestamp = str(row[1])[:10]
        confidence = row[2]
        cur.execute("INSERT INTO confidence_clean (coordinates, datetime, confidence) VALUES (?,?,?)", (coordinates, timestamp, confidence))
        conn.commit()

    try:
        cur.execute("CREATE TABLE weather_clean (query TEXT, datetime REAL, weather TEXT)")
    except:
        print("Table exists.")
    cur.execute("SELECT * From Weather")
    data = cur.fetchall()
    for row in data:
        query = row[0]
        timestamp = row[1]
        dt_object = datetime.fromtimestamp(timestamp)
        year_month = str(dt_object)[:10]
        main = row[2]
        cur.execute("INSERT INTO weather_clean (query, datetime, weather) VALUES (?,?,?)", (query, year_month, main))
        conn.commit()

# uncomment to clean 4 (weather, temp, traffic, confidence) data tables
# clean_data()

def take_traffic_averages():
    '''Input: None
    Function: takes average traffic speed per day and adds data to database
    Returns: dictionary with traffic averages per day, with the day as keys and the traffic average as values
    '''
    conn = sqlite3.connect('database.db')
    cur = conn.cursor()
    avgs = {}
    try:
        cur.execute("CREATE TABLE traffic_averages (query TEXT, datetime INT, avg_speed REAL)")
    except:
        print("Table exists.")
    cur.execute('SELECT datetime, speed FROM traffic_clean')
    data = cur.fetchall()
    for tup in data:
        date = int(tup[0].replace('-', ""))
        speed = tup[-1]
        if date not in avgs:
            avgs[date] = []
        else:
            avgs[date].append(speed)
    for date in avgs:
        sum_speed = 0
        counter = 0
        for speed in avgs[date]:
            counter += 1
            sum_speed += speed
        avg_speed = sum_speed/counter
        avgs[date] = avg_speed
        cur.execute('INSERT INTO traffic_averages (query, datetime, avg_speed) VALUES (?,?,?)', ('Ann Arbor', date, avg_speed))
        conn.commit()
    return avgs

#print(take_traffic_averages())

def take_temp_averages():
    '''Input: None
    Function: takes average temperature per day and adds data to database
    Returns: dictionary with temperature averages per day, with the day as keys and the temperature average as values
    '''
    conn = sqlite3.connect('database.db')
    cur = conn.cursor()
    avgs = {}
    try:
        cur.execute("CREATE TABLE temp_averages (query TEXT, datetime INT, temp REAL)")
    except:
        print("Table exists.")
    cur.execute('SELECT datetime, temp FROM temp_clean')
    data = cur.fetchall()
    for tup in data:
        date = int(tup[0].replace('-', ""))
        temp = tup[-1]
        if date not in avgs:
            avgs[date] = []
        else:
            avgs[date].append(temp)
    for date in avgs:
        sum_temp = 0
        counter = 0
        for temp in avgs[date]:
            counter += 1
            sum_temp += temp
        avg_temp = sum_temp/counter
        avgs[date] = avg_temp
        cur.execute('INSERT INTO temp_averages (query, datetime, temp) VALUES (?,?,?)', ('Ann Arbor', date, avg_temp))
        conn.commit()
    return avgs

# print(take_temp_averages())


def take_confidence_averages():
    '''Input: None
    Function: takes average confidence in traffic reports per day and adds data to database
    Returns: dictionary with average confidence in traffic reports per day, with the day as keys and the average confidence as values
    '''
    conn = sqlite3.connect('database.db')
    cur = conn.cursor()
    avgs = {}
    try:
        cur.execute("CREATE TABLE confidence_averages (query TEXT, datetime INT, confidence REAL)")
    except:
        print("Table exists.")
    cur.execute('SELECT datetime, confidence FROM confidence_clean')
    data = cur.fetchall()
    for tup in data:
        date = int(tup[0].replace('-', ""))
        conf = tup[-1]
        if date not in avgs:
            avgs[date] = []
        else:
            avgs[date].append(conf)
    for date in avgs:
        sum_conf = 0
        counter = 0
        for conf in avgs[date]:
            counter += 1
            sum_conf += conf
        avg_conf = sum_conf/counter
        avgs[date] = avg_conf
        cur.execute('INSERT INTO confidence_averages (query, datetime, confidence) VALUES (?,?,?)', ('Ann Arbor', date, avg_conf))
        conn.commit()
    return avgs

# print(take_confidence_averages())

def take_weather_averages():
    '''Input: None
    Function: takes average weather per day and adds data to database
    Returns: dictionary with average weather per day, with the day as keys and the average weather as values
    '''
    conn = sqlite3.connect('database.db')
    cur = conn.cursor()
    avgs = {}
    avg_dict = {}
    try:
        cur.execute("CREATE TABLE weather_averages (query TEXT, datetime INT, weather TEXT)")
    except:
        print("Table exists.")
    cur.execute('SELECT datetime, weather FROM weather_clean')
    data = cur.fetchall()
    for tup in data:
        date = int(tup[0].replace('-', ""))
        weather = tup[-1]
        if date not in avgs:
            avgs[date] = {}
        elif weather not in avgs[date]:
            avgs[date][weather] = 1
        else:
            avgs[date][weather] += 1
    for x in avgs.items():
        date = x[0]
        weather_dict = x[-1]
        if len(weather_dict) == 1:
            avg_dict[date] = weather 
        else:
            sorted_dict = sorted(weather_dict.items(), key=lambda x: x[-1], reverse=True)
            avg_dict[date] = sorted_dict[0][0]
    avg_list = avg_dict.items()
    for tup in avg_list:
        cur.execute('INSERT INTO weather_averages (query, datetime, weather) VALUES (?,?,?)', ('Ann Arbor', tup[0], tup[-1]))
        conn.commit()
    return avg_dict

# print(take_weather_averages())

def join_data():
    '''Input: None
    Function: joins data from various tables to create final_averages table in database
    Returns: dictionary with joined data
    '''
    conn = sqlite3.connect('database.db')
    cur = conn.cursor()
    cur.execute('SELECT * FROM temp_averages JOIN traffic_averages ON temp_averages.datetime = traffic_averages.datetime')
    data = cur.fetchall()
    try:
        cur.execute("CREATE TABLE averages (query TEXT, datetime INT, avg_temp REAL, avg_speed REAL)")
    except:
        print("Table exists.")
    for row in data:
        query = row[0]
        datetime = row[1]
        temp = row[2]
        speed = row[-1]
        cur.execute('INSERT INTO averages (query, datetime, avg_temp, avg_speed) VALUES (?,?,?,?)', (query, datetime, temp, speed))
        conn.commit()
    
    cur.execute('SELECT * FROM averages JOIN confidence_averages ON averages.datetime = confidence_averages.datetime')
    data = cur.fetchall()
    try:
        cur.execute("CREATE TABLE averages_2 (query TEXT, datetime INT, avg_temp REAL, avg_speed REAL, confidence REAL)")
    except:
        print("Table exists.")
    for row in data:
        query = row[0]
        datetime = row[1]
        temp = row[2]
        speed = row[3]
        confidence = row[-1]
        cur.execute('INSERT INTO averages_2 (query, datetime, avg_temp, avg_speed, confidence) VALUES (?,?,?,?,?)', (query, datetime, temp, speed, confidence))
        conn.commit()

    cur.execute('SELECT * FROM averages_2 JOIN weather_averages ON averages_2.datetime = weather_averages.datetime')
    data = cur.fetchall()
    try:
        cur.execute("CREATE TABLE final_averages (query TEXT, datetime INT, avg_temp REAL, avg_speed REAL, confidence REAL, weather TEXT)")
    except:
        print("Table exists.")
    for row in data:
        query = row[0]
        datetime = row[1]
        temp = row[2]
        speed = row[3]
        confidence = row[4]
        weather = row[-1]
        cur.execute('INSERT INTO final_averages (query, datetime, avg_temp, avg_speed, confidence, weather) VALUES (?,?,?,?,?,?)', (query, datetime, temp, speed, confidence, weather))
        conn.commit()

    return data

# calculations/create final averages table
# print(join_data())

def write_data(filename):
    '''Input: a filename
    Function: writes joined data to a text file
    Returns: None
    '''
    conn = sqlite3.connect('database.db')
    cur = conn.cursor()
    cur.execute('SELECT * FROM final_averages')
    data = cur.fetchall()
    with open(filename, mode='w') as write_file:
        write_file.write('(query, datetime, avg_temp, avg_speed, confidence, weather)')
        for row in data:
            write_file.write('\n')
            write_file.write(str(row))
            

#uncomment to write data to text file
write_data('data.txt')

def visualize_data():
    '''Input: None
    Function: creates 4 visualizations of the data: single line graph, double line graph, pie chart, and bar graph
    Returns: 4 plot images
    '''
    # weather per day
    snow_count = 0
    rain_count = 0
    cloud_count = 0
    conn = sqlite3.connect('database.db')
    cur = conn.cursor()
    cur.execute('SELECT datetime, weather FROM final_averages')
    data = cur.fetchall()
    for row in data:
        if row[-1] == 'Snow':
            snow_count += 1
        elif row[-1] == 'Rain':
            rain_count += 1
        elif row[-1] == 'Clouds':
            cloud_count += 1
    total = len(data)
    snow_percent = snow_count/total
    rain_percent = rain_count/total
    cloud_percent = cloud_count/total
    labels = ['Snow', 'Rain', 'Clouds']
    values = [snow_percent, rain_percent, cloud_percent]
    colors = ['#8C92AC', '#71A6D2', '#D3D3D3']
    fig, ax = plt.subplots()
    ax.pie(values, labels=labels, colors=colors, autopct='%1.1f%%')
    ax.axis('equal')
    plt.title('Average Weather Conditions from 12/4/19 - 12/11/19')
    plt.savefig('pie.png')
    plt.show()

    # confidence vs. avg_temp
    cur.execute('SELECT datetime, avg_temp, confidence FROM final_averages')
    dict_data = {}
    data = cur.fetchall()
    for row in data:
        datetime = row[0]
        if datetime not in dict_data:
            dict_data[datetime] = [(row[1], row[-1])]
        else:
            dict_data[datetime].append((row[1], row[-1]))
    for key in dict_data:
        sum_temp = 0
        sum_conf = 0
        denom = len(dict_data[key])
        for tup in dict_data[key]:
            sum_temp += tup[0]
            sum_conf += tup[1]
        avg_temp = sum_temp/denom
        avg_conf = sum_conf/denom
        dict_data[key] = (avg_temp, avg_conf)
    dates = dict_data.keys()
    temp_list = []
    conf_list = []
    for tup in dict_data.values():
        temp_list.append(tup[0])
        conf_list.append(tup[1])
    fig, (ax1, ax2) = plt.subplots(2)
    dates = list(dates)
    ax1.plot(dates, temp_list, label='Temperature')
    ax2.plot(dates, conf_list, color='r', label='Confidence')
    ax1.legend()
    ax1.grid()
    ax2.legend()
    ax2.grid()
    ax1.set_ylabel('Temperature')
    ax2.set_ylabel('Confidence')
    ax1.set_xticklabels((' ', '12-04', '12-05', '12-06', '12-07', '12-08', '12-09', '12-10', '12-11'))
    ax2.set_xticklabels((' ', '12-04', '12-05', '12-06', '12-07', '12-08', '12-09', '12-10', '12-11'))
    ax1.set_title('Average Temperature vs. Average Confidence in Traffic Predictions per day (12/4/19 - 12/11/19)')
    plt.xlabel('Day')
    fig.savefig('line.png')
    plt.show()
    
    # avg_temp vs. avg_speed
    cur.execute('SELECT datetime, avg_temp, avg_speed FROM final_averages')
    dict_data = {}
    data = cur.fetchall()
    for row in data:
        datetime = row[0]
        if datetime not in dict_data:
            dict_data[datetime] = [(row[1], row[-1])]
        else:
            dict_data[datetime].append((row[1], row[-1]))
    for key in dict_data:
        sum_temp = 0
        sum_speed = 0
        denom = len(dict_data[key])
        for tup in dict_data[key]:
            sum_temp += tup[0]
            sum_speed += tup[1]
        avg_temp = sum_temp/denom
        avg_speed = sum_speed/denom
        dict_data[key] = (avg_temp, avg_speed)
    dates = dict_data.keys()
    temp_list = []
    speed_list = []
    for tup in dict_data.values():
        temp_list.append(tup[0])
        speed_list.append(tup[1])
    fig, ax = plt.subplots()
    dates = list(dates)
    ax.plot(dates, temp_list, label='Temperature')
    ax.plot(dates, speed_list, color='green', label='Speed')
    ax.legend()
    ax.grid()
    ax.set_xticklabels((' ', '12-04', '12-05', '12-06', '12-07', '12-08', '12-09', '12-10', '12-11'))
    ax.set_title('Average Temperature vs. Average Speed per day (12/4/19 - 12/11/19)')
    plt.xlabel('Day')
    fig.savefig('doubleline.png')
    plt.show()

    # confidence vs. weather
    cur.execute('SELECT confidence, weather FROM final_averages')
    dict_data = {}
    graph_data = {}
    data = cur.fetchall()
    for row in data:
        confidence = row[0]
        weather = row[1]
        if weather not in dict_data:
            dict_data[weather] = [confidence]
        else:
            dict_data[weather].append(confidence)
    for tup in dict_data.items():
        sum_conf = 0
        denom = len(tup[-1])
        for val in tup[-1]:
            sum_conf += val
        avg_conf = sum_conf/denom
        graph_data[tup[0]] = avg_conf
    weather_conditions = graph_data.keys()
    confidence_vals = graph_data.values()
    fig, ax = plt.subplots()
    ax.bar(weather_conditions, confidence_vals, color=['#D3D3D3', '#71A6D2', '#8C92AC'])
    ax.set_xticklabels(('Clouds', 'Rain', 'Snow'))
    ax.set_ylabel('Confidence')
    ax.set_yticks([.90, .905, .91, .915, .92, .925, .93, .935])
    ax.set_ylim(0.9, 0.94)
    ax.set_title('Average Confidence Level in Traffic Reports Given Weather Conditions')
    plt.xlabel('Weather Condition')
    fig.savefig('bar.png')
    plt.show()

#uncomment to create data visualizations 
# print(visualize_data())

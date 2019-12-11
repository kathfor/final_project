import json, requests
import sqlite3
from datetime import datetime

#Weather API
def weather_conditions(query):
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
        print('\n')
        conn.commit()
    return None

def report_confidence(coord_lst):
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
        print('\n')
        conn.commit()
    return None

#uncomment to collect traffic data
# print(traffic_Data(coordinates))
# print(report_confidence(coordinates))

#
def clean_data():
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
    
clean_data()

def take_traffic_averages():
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

# print(take_weather_averages())

def join_data():
    conn = sqlite3.connect('database.db')
    cur = conn.cursor()
    # cur.execute('SELECT * FROM temp_averages JOIN traffic_averages ON temp_averages.datetime = traffic_averages.datetime')
    # data = cur.fetchall()
    # try:
    #     cur.execute("CREATE TABLE averages (query TEXT, datetime INT, avg_temp REAL, avg_speed REAL)")
    # except:
    #     print("Table exists.")
    # for row in data:
    #     query = row[0]
    #     datetime = row[1]
    #     temp = row[2]
    #     speed = row[-1]
    #     cur.execute('INSERT INTO averages (query, datetime, avg_temp, avg_speed) VALUES (?,?,?,?)', (query, datetime, temp, speed))
    #     conn.commit()
    
    # cur.execute('SELECT * FROM averages JOIN confidence_averages ON averages.datetime = confidence_averages.datetime')
    # data = cur.fetchall()
    # try:
    #     cur.execute("CREATE TABLE averages_2 (query TEXT, datetime INT, avg_temp REAL, avg_speed REAL, confidence REAL)")
    # except:
    #     print("Table exists.")
    # for row in data:
    #     query = row[0]
    #     datetime = row[1]
    #     temp = row[2]
    #     speed = row[3]
    #     confidence = row[-1]
    #     cur.execute('INSERT INTO averages_2 (query, datetime, avg_temp, avg_speed, confidence) VALUES (?,?,?,?,?)', (query, datetime, temp, speed, confidence))
    #     conn.commit()

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

print(join_data())


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
# -*- coding: UTF-8 -*-
import json
import os
import re

from pyspark import SparkContext
from pyspark.sql import SparkSession
from operator import add

semantic_types = ["Person Name", "Business name", "Phone Number", "Address", "Street name", "City", "Neighborhood", "LAT/LON coordinates", "Zip code", "Borough", "School name", "Color", "Car make", "City agency", "Areas of study", "Subjects in school", "School Levels", "College/University names", "Websites", "Building Classification", "Vehicle Type", "Type of location", "Parks/Playgrounds", "other"]

def initLists():
    global cities
    global color
    global car_make
    city = sc.textFile("uscities.csv")
    cities = city.map(lambda x: x.split(',')[0].strip('\"')).collect()
    car_make = ['abarth', 'alfa romeo', 'aston martin', 'audi', 'bentley', 'bmw', 'bugatti', 'cadillac', 'chevrolet', 'chrysler', 'citroen', 'dacia', 'daewoo', 'daihatsu', 'dodge', 'donkervoort', 'ds', 'ferrari', 'fiat', 'fisker', 'ford', 'honda', 'hummer', 'hyundai', 'infiniti', 'iveco', 'jaguar', 'jeep', 'kia', 'ktm', 'lada', 'lamborghini', 'lancia', 'land rover', 'landwind', 'lexus', 'lotus', 'maserati', 'maybach', 'mazda', 'mclaren', 'mercedes-benz', 'mg', 'mini', 'mitsubishi', 'morgan', 'nissan', 'opel', 'peugeot', 'porsche', 'renault', 'rolls-royce', 'rover', 'saab', 'seat', 'skoda', 'smart', 'ssangyong', 'subaru', 'suzuki', 'tesla', 'toyota', 'volkswagen', 'volvo']


borough = "bronx, brooklyn, manhattan, queens, staten island"


class Column:
    column_name = ""
    semantic_types = []

    def __init__(self, name):
        self.column_name = name

class SemanticType:
    semantic_type = ""
    label = ""
    count = 0

    def __init__(self, type, label, count):
        self.semantic_type = type
        self.label = label
        self.count = count

class MyEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, bytes):
            return str(obj, encoding='utf-8')
        return json.JSONEncoder.default(self, obj)


def mkdir(path):
    folder = os.path.exists(path)
    if not folder:
        os.makedirs(path)


def getSemanticType(keyword):
    if keyword is None or len(keyword) == 0:
        return -1
    # Person name (Last name, First name, Middle name, Full name) 0
    # ● Business name 1
    # ● Phone Number 2
    if re.match(re.compile(r'^[\d]{9,9}$'), re.sub(r'\D', "", keyword)):
        return 2
    # ● Address 3
    # ● Street name 4
    # ● City 5
    elif keyword.lower() in cities:
        return 5
    # ● Neighborhood 6
    # ● LAT/LON coordinates 7
    elif re.match(re.compile(r'^[-+]?([1-8]?\d(\.\d+)?|90(\.0+)?),\s*[-+]?(180(\.0+)?|((1[0-7]\d)|([1-9]?\d))(\.\d+)?)$'), keyword):
        return 7
    # ● Zip code 8
    elif re.match(re.compile(r'^[\d]{5,5}$'), keyword):
        return 8
    # ● Borough 9
    elif keyword.lower() in borough or keyword.lower() in borough:
        return 9
    # ● School name (Abbreviations and full names) 10
    # ● Color 11
    # ● Car make 12
    elif keyword.lower() in car_make:
        return 12
    # ● City agency (Abbreviations and full names) 13
    # ● Areas of study (e.g., Architecture, Animal Science, Communications) 14
    # ● Subjects in school (e.g., MATH A, MATH B, US HISTORY) 15
    # ● School Levels (K-2, ELEMENTARY, ELEMENTARY SCHOOL, MIDDLE) 16
    # ● College/University names 17
    # ● Websites (e.g., ASESCHOLARS.ORG) 18
    elif re.match(re.compile(r'^https?:\/\/(?:www\.|(?!www))[a-zA-Z0-9][a-zA-Z0-9-]+[a-zA-Z0-9]\.[^\s]{2,}|www\.[a-zA-Z0-9][a-zA-Z0-9-]+[a-zA-Z0-9]\.[^\s]{2,}|https?:\/\/(?:www\.|(?!www))[a-zA-Z0-9]+\.[^\s]{2,}|www\.[a-zA-Z0-9]+\.[^\s]{2,}$'), keyword):
        return 18
    # ● Building Classification (e.g., R0-CONDOMINIUM, R2-WALK-UP)
    # ● Vehicle Type (e.g., AMBULANCE, VAN, TAXI, BUS)
    # ● Type of location (e.g., ABANDONED BUILDING, AIRPORT TERMINAL, BANK, CHURCH, CLOTHING/BOUTIQUE)
    # ● Parks/Playgrounds (e.g., CLOVE LAKES PARK, GREENE PLAYGROUND)
    else:
        return -1

def checkSemanticType(input):
    if input is None:
        return (('other', 'None'), 1)
    key = input[0].strip()
    result = ['', '', input[1]]
    result[0] = semantic_types[getSemanticType(key)]
    # if result[0] == "other":
    #     result[1] = key
    return ((result[0], result[1]), result[2])

if __name__ == "__main__":
    sc = SparkContext()
    initLists()

    # /user/hm74/NYCOpenData/
    path = "./NYCColumns/"

    cluster = open("cluster1.txt", 'r')
    task2_files = [file.strip().strip('\'') for file in cluster.read().strip('[]').split(',')]
    cluster.close()

    column_list = []
    for file in os.listdir(path):
        if file.startswith('.') or file not in task2_files or ("car" not in file.lower() and "vehicle" not in file.lower()):
            continue
        print("Processing File %s" % file)
        currColumn = Column(file.split('.')[1])
        column = sc.textFile(path + file)
        column = column.map(lambda x: (x.split("\t")[0], int(x.split("\t")[1]))) \
                       .map(lambda x: checkSemanticType(x)) \
                       .reduceByKey(add) \
                       .sortBy(lambda x: -x[1])
        print(column.collect())
        currColumn.semantic_types = column.map(lambda x: SemanticType(x[0][0], x[0][1], x[1])).collect()
        column_list.append(currColumn)
        print("File %s finish" % file)

    print(column_list)
    sc.stop()




























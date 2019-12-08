# -*- coding: UTF-8 -*-
import json
import os
import re

from pyspark import SparkContext
from pyspark.sql import SparkSession
from operator import add
from fuzzywuzzy import process

semantic_types = ["Person Name", "Business name", "Phone Number", "Address", "Street name", "City", "Neighborhood", "LAT/LON coordinates", "Zip code", "Borough", "School name", "Color", "Car make", "City agency", "Areas of study", "Subjects in school", "School Levels", "College/University names", "Websites", "Building Classification", "Vehicle Type", "Type of location", "Parks/Playgrounds", "other"]
threshold = 80

def initLists():
    global cities, color, car_make, borough, school_level, building_class, vehicle_type, subjects, color, areas, neighbor, agencies

    city = sc.textFile("uscities.csv")
    cities = city.map(lambda x: x.split(',')[0].strip('\"')).collect()
    neighbor = sc.textFile("neighborhood.txt").collect()
    car_make = ['abarth', 'alfa romeo', 'aston martin', 'audi', 'bentley', 'bmw', 'bugatti', 'cadillac', 'chevrolet', 'chrysler', 'citroen', 'dacia', 'daewoo', 'daihatsu', 'dodge', 'donkervoort', 'ds', 'ferrari', 'fiat', 'fisker', 'ford', 'honda', 'hummer', 'hyundai', 'infiniti', 'iveco', 'jaguar', 'jeep', 'kia', 'ktm', 'lada', 'lamborghini', 'lancia', 'land rover', 'landwind', 'lexus', 'lotus', 'maserati', 'maybach', 'mazda', 'mclaren', 'mercedes-benz', 'mg', 'mini', 'mitsubishi', 'morgan', 'nissan', 'opel', 'peugeot', 'porsche', 'renault', 'rolls-royce', 'rover', 'saab', 'seat', 'skoda', 'smart', 'ssangyong', 'subaru', 'suzuki', 'tesla', 'toyota', 'volkswagen', 'volvo']
    borough = ['bronx', 'brooklyn', 'manhattan', 'queens', 'staten island']
    school_level = ['middle', 'elementary', 'high', 'k-2', 'k-3', 'k-4', 'k-5', 'k-6', 'k-7', 'k-8', 'k-9', 'k-10', 'k-11', 'k-12'] # transfer school ?
    building_class = ['r0-condominium', 'r2-walk-up', 'r9-condops', 'c1-walk-up', 'c2-walk-up', 'c3-walk-up', 'c4-walk-up', 'c5-walk-up', 'c6-walk-up', 'c7-walk-up', 'c8-walk-up', 'd0-elevator', 'd1-elevator', 'd2-elevator', 'd3-elevator', 'd4-elevator', 'd5-elevator', 'd6-elevator', 'd7-elevator', 'd8-elevator', 'd9-elevator']
    vehicle_type = ['sedan', 'ambulance', 'truck', 'bicycle', 'bus', 'convertible', 'motorcycle', 'vehicle', 'moped', 'scooter', 'taxi', 'pedicab', 'boat', 'van', 'bike', 'tank']
    subjects = ['algebra', 'chemistry', 'earth science', 'economics', 'english', 'geometry', 'global history', 'living environment', 'physics', 'math', 'math a', 'math b', 'us history', 'science', 'us government', 'us government & economics', 'us history', 'social studies']
    color = ['amber', 'apricot', 'aqua', 'auburn', 'azure', 'beige', 'black', 'blue', 'bronze', 'brown', 'burgundy', 'charcoal', 'cherry blossom pink', 'chocolate', 'cobalt', 'copper', 'cream', 'crimson', 'cyan', 'dandelion', 'dark', 'denim', 'ecru', 'emerald green', 'forest green', 'fuchsia', 'gold', 'green', 'grey', 'indigo', 'ivory', 'jade', 'khaki', 'lavender', 'lemon', 'lilac', 'lime green', 'magenta', 'maroon', 'mauve', 'mint green', 'moss green', 'mustard', 'navy blue', 'olive', 'orange', 'peach', 'pink', 'powder blue', 'puce', 'prussian blue', 'purple', 'quartz grey', 'red', 'rose', 'royal blue', 'ruby', 'salmon pink', 'sandy brown', 'sapphire', 'scarlet', 'shocking pink', 'silver', 'sky blue', 'tan', 'tangerine', 'turquoise', 'violet', 'white', 'yellow']
    areas = ['animal science', 'architecture', 'business', 'communications', 'computer science & technology', 'cosmetology', 'culinary arts', 'engineering', 'environmental science', 'film/video', 'health professions', 'hospitality, travel, & tourism', 'humanities & interdisciplinary', 'jrotc', 'law & government', 'performing arts', 'performing arts/visual art & design', 'science & math', 'teaching', 'visual art & design', 'zoned']
    agencies = ["Actuary, NYC Office of the (NYCOA)","Administrative Justice Coordinator, NYC Office of (AJC)","Administrative Tax Appeals, Office of","Administrative Trials and Hearings, Office of (OATH)","Aging, Department for the (DFTA)","Appointments, Mayor's Office of (MOA)","Brooklyn Public Library (BPL)","Buildings, Department of (DOB)","Business Integrity Commission (BIC)","Campaign Finance Board (CFB)","Center for Innovation through Data Intelligence (CIDI)","Charter Revision Commission","Chief Medical Examiner, NYC Office of (OCME)","Children's Services, Administration for (ACS)","City Clerk, Office of the (CLERK)","City Council, New York","City Planning, Department of (DCP)","City University of New York (CUNY)","Citywide Administrative Services, Department of (DCAS)","Citywide Event Coordination and Management, Office of (CECM)","Civic Engagement Commission (CEC)","Civil Service Commission (CSC)","Civilian Complaint Review Board (CCRB)","Climate Policy & Programs","Commission on Gender Equity (CGE)","Commission to Combat Police Corruption (CCPC)","Community Affairs Unit (CAU)","Community Boards (CB)","Comptroller (COMP)","Conflicts of Interest Board (COIB)","Consumer Affairs, Department of (DCA)","Consumer and Worker Protection, Department of (DCWP)","Contract Services, Mayor's Office of (MOCS)","Correction, Board of (BOC)","Correction, Department of (DOC)","Criminal Justice, Mayor's Office of","Cultural Affairs, Department of (DCLA)","Data Analytics, Mayor's Office of (MODA)","Design and Construction, Department of (DDC)","District Attorney - Bronx County","District Attorney - Kings County (Brooklyn)","District Attorney - New York County (Manhattan)","District Attorney - Queens County","District Attorney - Richmond County (Staten Island)","Education, Department of (DOE)","Elections, Board of (BOE)","Emergency Management, NYC","Environmental Coordination, Mayor’s Office of (MOEC)","Environmental Protection, Department of (DEP)","Equal Employment Practices Commission (EEPC)","Finance, Department of (DOF)","Fire Department, New York City (FDNY)","Fiscal Year 2005 Securitization Corporation","Food Policy Director, Office of the","GreeNYC (GNYC)","Health and Mental Hygiene, Department of (DOHMH)","Homeless Services, Department of (DHS)","Housing Authority, New York City (NYCHA)","Housing Preservation and Development, Department of (HPD)","Housing Recovery Operations (HRO)","Hudson Yards Infrastructure Corporation","Human Resources Administration (HRA)","Human Rights, City Commission on (CCHR)","Immigrant Affairs, Mayor's Office of (MOIA)","Independent Budget Office, NYC (IBO)","Information Privacy, Mayor's Office of (MOIP)","Information Technology and Telecommunications, Department of (DOITT)","Inspector General NYPD, Office of the","Intergovernmental Affairs, Mayor's Office of (MOIGA)","Investigation, Department of (DOI)","Judiciary, Mayor's Advisory Committee on the (MACJ)","Labor Relations, NYC Office of (OLR)","Landmarks Preservation Commission (LPC)","Law Department (LAW)","Library, Brooklyn Public (BPL)","Library, New York Public (NYPL)","Library, Queens Public (QL)","Loft Board (LOFT)","Management and Budget, Office of (OMB)","Mayor's Committee on City Marshals (MCCM)","Mayor's Fund to Advance NYC (Mayor's Fund)","Mayor's Office (OM)","Mayor's Office for Economic Opportunity","Mayor's Office for International Affairs (IA)","Mayor's Office for People with Disabilities (MOPD)","Mayor's Office of Environmental Remediation (OER)","Mayor's Office of Special Projects & Community Events (MOSPCE)","Mayor's Office of the Chief Technology Officer","Mayor’s Office of Minority and Women-Owned Business Enterprises (OMWBE)","Mayor’s Office of Strategic Partnerships (OSP)","Mayor’s Office to End Domestic and Gender-Based Violence (ENDGBV)","Media and Entertainment, Mayor's Office of (MOME)","Media, NYC","NYC & Company (NYCGO)","NYC Children's Cabinet","NYC Cyber Command","NYC Economic Development Corporation (NYCEDC)","NYC Employees' Retirement System (NYCERS)","NYC Health + Hospitals","NYC Service (SERVICE)","NYC Young Men’s Initiative","New York City Transitional Finance Authority (TFA)","New York Public Library (NYPL)","Office of Recovery & Resiliency","Office of ThriveNYC","Office of the Census for NYC","Operations, Mayor's Office of (OPS)","Parks and Recreation, Department of (DPR)","Payroll Administration, Office of (OPA)","Police Department (NYPD)","Police Pension Fund (PPF)","Probation, Department of (DOP)","Procurement Policy Board (PPB)","Property Tax Reform, Advisory Commission on","Public Administrator - Bronx County (BCPA)","Public Administrator - Kings County (KCPA)","Public Administrator - New York County (NYCountyPA)","Public Administrator - Queens County (QPA)","Public Administrator - Richmond County (RCPA)","Public Advocate (PUB ADV)","Public Design Commission","Queens Public Library (QPL)","Records and Information Services, Department of (DORIS)","Rent Guidelines Board (RGB)","Sales Tax Asset Receivable Corporation (STAR)","Sanitation, Department of (DSNY)","School Construction Authority (SCA)","Small Business Services (SBS)","Social Services, Department of (DSS)","Special Commissioner of Investigation for the New York City School District","Special Enforcement, Mayor’s Office of (OSE)","Special Narcotics Prosecutor, NYC Office of the (SNP)","Standards and Appeals, Board of (BSA)","Sustainability, Mayor's Office Of","TSASC, Inc.","Tax Appeals Tribunal, New York City (TAT)","Tax Commission, New York City (TC)","Taxi and Limousine Commission (TLC)","Teachers' Retirement System of the City of New York","Transportation, Department of (DOT)","Veterans' Services, Department of (DVS)","Water Board (NYWB)","Water Finance Authority, NYC Municipal (NYW)","Workforce Development, Mayor's Office of","Youth and Community Development, Department of (DYCD)", "NYCOA", "AJC", "OATH", "DFTA", "MOA", "BPL", "DOB", 'BIC', 'CFB', 'CIDI', 'OCME', 'ACS', 'CLERK', 'DCP', 'CUNY', 'DCAS', 'CECM', 'CEC', 'CSC', 'CCRB', 'CGE', 'CCPC', 'CAU', 'CB', 'COMP', 'COIB', 'DCA', 'DCWP', 'MOCS', 'BOC', 'DOC', 'DCLA', 'MODA', 'DDC', 'DOE', 'BOE', 'MOEC', 'DEP', 'EEPC', 'DOF', 'FDNY', 'GNYC', 'DOHMH', 'DHS', 'NYCHA', 'HPD', 'HRO', 'HRA', 'CCHR', 'MOIA', 'IBO', 'MOIP', 'DOITT', 'MOIGA', 'DOI', 'MACJ', 'OLR', 'LPC', 'LAW', 'BPL', 'NYPL', 'QL', 'LOFT', 'OMB', 'MCCM', 'OM', 'IA', 'MOPD', 'OER', 'MOSPCE', 'OMWBE', 'OSP', 'ENDGBV', 'MOME', 'NYCGO', 'NYCEDC', 'NYCERS', 'SERVICE', 'TFA', 'NYPL', 'OPS', 'DPR', 'OPA', 'NYPD', 'PPF', 'DOP', 'PPB', 'BCPA', 'KCPA', 'QPA', 'RCPA', 'PUB ADV', 'QPL', 'DORIS', 'RGB', 'STAR', 'DSNY', 'SCA', 'SBS', 'DSS', 'OSE', 'SNP', 'BSA', 'TAT', 'TC', 'TLC', 'DOT', 'DVS', 'NYWB', 'NYW', 'DYCD', '311']


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


def checkItemInList(keyword, keyword_list):
    matched = process.extractOne(keyword, keyword_list)
    return matched
    # if matched[1] > threshold:
    #     return True
    # else:
    #     return False


def getSemanticType(keyword):
    if keyword is None or len(keyword) == 0:
        return -1
    # Person name (Last name, First name, Middle name, Full name) 0
    # ● Business name 1
    # ● Phone Number 2
    if re.match(re.compile(r'1?[ -\.]*[0-9]{3}[ -\.]*[0-9]{3}[ -\.]*[0-9]{4}|.*\(.*[0-9]{3}.*\).*[0-9]{3}.*-.*[0-9]{4}'), keyword):
        return 2
    # ● Address 3
    # ● Street name 4
    # ● City 5
    elif keyword in cities:
        return 5
    # ● Neighborhood 6
    elif checkItemInList(keyword, neighbor):
        return 6
    # ● LAT/LON coordinates 7
    elif re.match(re.compile(r'\(?-?[0-9]{1,3}\.?[0-9]*, *-?[0-9]{1,3}\.?[0-9]*\)?'), keyword):
        group = re.findall(r'-?[0-9]{1,3}\.?[0-9]*', keyword)
        if (float(group[0]) > 180.0 or float(group[0]) < -180.0) or (float(group[1]) > 180.0 or float(group[1]) < -180.0):
            return -1
        return 7
    # ● Zip code 8
    elif re.match(re.compile(r'[0-9]{5}|[0-9]{5}-[0-9]{4}'), keyword):
        return 8
    # ● Borough 9
    elif checkItemInList(keyword, borough):
        return 9
    # ● School name (Abbreviations and full names) 10
    # ● Color 11
    elif checkItemInList(keyword, color):
        return 11
    # ● Car make 12
    elif keyword in car_make:
        return 12
    # ● City agency (Abbreviations and full names) 13
    elif checkItemInList(keyword, agencies):
        return 13
    # ● Areas of study (e.g., Architecture, Animal Science, Communications) 14
    elif checkItemInList(keyword, areas):
        return 14
    # ● Subjects in school (e.g., MATH A, MATH B, US HISTORY) 15
    elif checkItemInList(keyword, subjects):
        return 15
    # ● School Levels (K-2, ELEMENTARY, ELEMENTARY SCHOOL, MIDDLE) 16
    elif checkItemInList(keyword, school_level):
        return 16
    # ● College/University names 17
    # ● Websites (e.g., ASESCHOLARS.ORG) 18
    elif re.match(re.compile(r'http[s]?:\/\/|www\.|[a-z0-9\.\-_]*\.org|[a-z0-9\.\-_]*\.com|[a-z0-9\.\-_]*\.edu|[a-z0-9\.\-_]*\.gov|[a-z0-9\.\-_]*\.net|[a-z0-9\.\-_]*\.info|[a-z0-9\.\-_]*\.us|[a-z0-9\.\-_]*\.nyc'), keyword):
        return 18
    # ● Building Classification (e.g., R0-CONDOMINIUM, R2-WALK-UP) 19
    elif checkItemInList(keyword, building_class):
        return 19
    # ● Vehicle Type (e.g., AMBULANCE, VAN, TAXI, BUS) 20
    elif checkItemInList(keyword, vehicle_type):
        return 20
    # ● Type of location (e.g., ABANDONED BUILDING, AIRPORT TERMINAL, BANK, CHURCH, CLOTHING/BOUTIQUE)
    # ● Parks/Playgrounds (e.g., CLOVE LAKES PARK, GREENE PLAYGROUND) 22
    else:
        return -1

def checkSemanticType(input):
    if input is None:
        return (('other', 'None'), 1)
    key = input[0].strip()
    result = ['', key, input[1]]
    # index = getSemanticType(key.lower())
    # result[0] = semantic_types[index]
    result[0], result[2] = checkItemInList(key.lower(), neighbor)
    if result[2] > 70:
        result[0] = semantic_types[6]
    else:
        result[0] = semantic_types[-1]
    # if result[0] == "other":
    #     result[1] = key
    return ((result[0], result[1]), result[2])

# def chekColumnName(column_name):
#     if "first" in name:
#         return "Name"


if __name__ == "__main__":
    sc = SparkContext()
    initLists()

    # /user/hm74/NYCOpenData/
    path = "./NYCColumns/"

    cluster = open("cluster1.txt", 'r')
    task2_files = [file.strip().strip('\'') for file in cluster.read().strip('[]').split(',')]
    cluster.close()

    column_list = []
    count = 0
    for file in os.listdir(path):
        if file.startswith('.') or file not in task2_files or "neighbor" not in file.lower():
            continue
        print("Processing File %s" % file)
        count += 1
        currColumn = Column(file.split('.')[1])
        column = sc.textFile(path + file)
        column = column.map(lambda x: (x.split("\t")[0], int(x.split("\t")[1]))) \
                       .map(lambda x: checkSemanticType(x)) \
                       .reduceByKey(add) \
                       .sortBy(lambda x: -x[1])
        items = column.collect()
        for item in items:
            print item
        # currColumn.semantic_types = column.map(lambda x: SemanticType(x[0][0], x[0][1], x[1])).collect()
        # column_list.append(currColumn)
        print("File %s finish" % file)
        if count > 5:
            break

    # print(column_list)
    sc.stop()
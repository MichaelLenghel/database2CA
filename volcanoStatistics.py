import pymongo as db
import csv
from pprint import pprint

myclient = db.MongoClient("mongodb://localhost:27017/")
# testdb is what ever youcalled your db with the use word
volcanodb = myclient["volcanoStatistics"]

eruptions = volcanodb["Volcanos"]

# Volcanos table -> Drop than add to db
# volcanodb.drop()

#Flat upload is done:
with open('volcanoEruptions.csv') as csv_file:
    csv_reader = csv.reader(csv_file, delimiter=',')
    line_count = 0
    for row in csv_reader:
        if line_count == 0:
            #print(f'Column names are {", ".join(row)}')
            line_count += 1
        else:
            # Columns with indexses:
            #0		1		2		3						4					  5			6		7			8			9			10		11		12		13					14						15		16		17						18			19					20				21						22						23					24				25								26				27							28				29							30				31						32									33							34						35									
        #   "Year","Month","Day","Associated Tsunami?","Associated Earthquake?","Name","Location","Country","Latitude","Longitude","Elevation","Type","Status","Time","Volcano Explosivity Index (VEI)","Agent","DEATHS","DEATHS_DESCRIPTION","MISSING","MISSING_DESCRIPTION","INJURIES","INJURIES_DESCRIPTION","DAMAGE_MILLIONS_DOLLARS","DAMAGE_DESCRIPTION","HOUSES_DESTROYED","HOUSES_DESTROYED_DESCRIPTION","TOTAL_DEATHS","TOTAL_DEATHS_DESCRIPTION","TOTAL_MISSING","TOTAL_MISSING_DESCRIPTION","TOTAL_INJURIES","TOTAL_INJURIES_DESCRIPTION","TOTAL_DAMAGE_MILLIONS_DOLLARS","TOTAL_DAMAGE_DESCRIPTION","TOTAL_HOUSES_DESTROYED","TOTAL_HOUSES_DESTROYED_DESCRIPTION"
            vol =  {	
        				"id": line_count,
            			"year": row[0],
            			"month": row[1],
            			"day": row[2],
            			"name": row[5],
            			"country": row[7],
            			"vei": row[14],
            			"deaths": row[16],
            			"missing": row[18],
            			"injuries": row[20],
            			"cost_damage": row[22]
	            	}
            line_count += 1
            x = eruptions.insert_one(vol)
    print(f'Processed {line_count} volcanos.')





cursor = eruptions.find()
for doc in cursor:
	pprint(doc)
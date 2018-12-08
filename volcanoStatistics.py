import pymongo as db
import csv
import collections
from pprint import pprint

# Set default varialbes for dictionary to an empty string
countryNames = collections.defaultdict(lambda: True)
# volcanos = collections.defaultdict(lambda: '')

myclient = db.MongoClient("mongodb://localhost:27017/")
# testdb is what ever youcalled your db with the use word
volcanodb = myclient["volcanoStatistics"]

# Declare collections
volcanoStatistics = volcanodb["Volcanos"]
volcanoObject = volcanodb["Volcanos"]
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
            			"location": row[6],
            			"country": row[7],
            			"latitude": row[8],
            			"longitude": row[9],
            			"vei": row[14],
            			"deaths": row[16],
            			"missing": row[18],
            			"injuries": row[20],
            			"cost_damage": row[22]
	            	}
            x = volcanoStatistics.insert_one(vol)

# cursor = volcanoStatistics.find()
# for volcanos in cursor:
# 	print("Column: " + volcanos["country"])

# Create mongo db structure
cursor = volcanoStatistics.find()
for volcanos in cursor:
	# Checks if countryName is in it
	if countryNames[volcanos["country"]]:
		print(volcanos["country"])
		volcanoRec ={
						"id": volcanos["id"],
						"country": volcanos["country"],
						# "latitude": volcanos["latitude"],
						# "longitude": volcanos["longitude"]	
					}
		x = volcanoObject.insert_one(volcanoRec)
		countryNames[volcanos["country"]] = False
	else:
		pass



		# Populate the countryNames first time, we don't want to re-add the same data next time
		
		# here we push an update to the flatVolcano

	# We add a new eruption record regardless
# 	eruptionRec ={
# 					"year": volcanos["day"],
# 					"month": volcanos["month"],
# 					"day": volcanos["month"],
# 					"vei": volcanos["vei"],
# 					"missing": volcanos["missing"],
#             		"injuries": volcanos["injuries"],
#             		"cost_damage": volcanos["cost_damage"],
# 					"deaths": volcanos["deaths"]
# 				}
# 	x = eruptions.insert_one(eruptionRec)

# cursor = volcanoObject.find()
# for doc in cursor:
# 	pprint(doc)
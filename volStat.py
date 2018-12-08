import pymongo as db
import csv
from pprint import pprint

# Set db connection using the localhost
myclient = db.MongoClient("mongodb://localhost:27017/")
# volcanodb is the name of our db
volcanodb = myclient["volcanoStatistics"]

# Will be the single data that holds static data for volcanos -> Country name, latitude and longitude, etc. with references to the eruptions
volcanoes = volcanodb["VolcanoObjs"]
# Will hold the variable data for eruptions, for example number of deaths, date, etc.
eruptions = volcanodb["Eruptions"]

volnames = {}

volcanoes.drop()
eruptions.drop()

#Flat upload is done:
with open('volcanoEruptions.csv') as csv_file:
    csv_reader = csv.reader(csv_file, delimiter=',')
    line_count = 0
    for row in csv_reader:
        # Do not print the headers in the csv file
        if line_count == 0:
            line_count += 1
        else:
            # Insert the base volcano object
            if row[5] not in volnames:
                vol =  {	
            				"Name" : row[5],
                            "Location" : row[6],
                            "Country" : row[7],
                            "Latitude" : row[8],
                            "Longitude" : row[9],
                            "Elevation" : row[10],
                            "Eruptions" : [line_count]
    	            	}
                # Add the volcano name to the dictionary
                volnames[row[5]] = row[5]
                # Insert into volcanoes collections the object above
                volcanoes.insert_one(vol)
            else:
                # Variable data that changes (deaths, date, etc.)
                eruption = {
                            "_id" : line_count,
                            "Year": row[0],
                            "VEI": row[14],
                            "Deaths": row[16]
                }
                eruptions.insert_one(eruption)
                # Add the reference. Volcanso hold references to the eruptions through a list of ids
                volcanoes.update({'Name': row[5]}, {'$push': {'Eruptions': line_count}})
            line_count += 1

# Print all volcanos out
cursor = volcanoes.find()
for vol in cursor:
    pprint(vol)

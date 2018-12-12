# Student Name: Michael Lenghe
# Student Number: C16434974

# Schema Design: Volcanoes contains all the indivudal volcanoes with static data (Location, latitude, longtitude, country)
# Erruptions holds variable data (Number of deaths caused, VEI, year it occured)

import pymongo as db
import csv
from pprint import pprint
from bson.son import SON

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
                # Deaths has '' (Empty) variables which mess up int casting (important for sorting), and cannot write internal 
                # if statements inside dictionary, so forced to separate code as seen below
                if row[16] == '':
                    eruption = {
                            "_id" : line_count,
                            "Year": row[0],
                            "VEI": row[14],
                            "Deaths": row[16]
                            # Time
                            # Status
                            # Type
                    }
                else:
                    eruption = {
                                "_id" : line_count,
                                "Year": row[0],
                                "VEI": row[14],
                                "Deaths": int(row[16])
                    }
                eruptions.insert_one(eruption)
                # Add the reference. Volcanso hold references to the eruptions through a list of ids
                volcanoes.update_one({'Name': row[5]}, {'$push': {'Eruptions': line_count}})
            line_count += 1


#### Query 1: Show all documents in the colleciton ####
print("Volcano collection: ")

# Print all volcanos out
cursor = volcanoes.find()
for vol in cursor:
    pprint(vol)

# Print all eruptions out
print("\nEruptions collection: ")
cursor = eruptions.find()
for erup in cursor:
    pprint(erup)

#### Query 2: Embedded array data based on selected criteria ####
print("\nShowing embedded array data through showing all the eruptions for a volcano using embedded aray: ")

#### Goal: Want all the data of the eruptions that occured to the volcano Pago (Which holds the ids as embedded arrays) ####
# Grab the volcano object that is named Pago
volcano_to_query = "Pago"
volcano = volcanoes.find_one({"Name": volcano_to_query} )
# Grab all eruption ids embedded in the Pago volcano object
print("Eruptions for " + volcano_to_query +"\nCountry Name: " + volcano["Country"])
for eruption_id in volcano["Eruptions"]:
    # Grab the eruption object with the specified eruption id
    eruption = eruptions.find_one({"_id": eruption_id})
    pprint(eruption)

#### Query 3: projection ####
print("\nShowing projection (Gets the volcano named Masaya): ")

volcanoCursor = volcanoes.find({"Name": "Masaya"} )
for vol in volcanoCursor:
    pprint(vol)

#### Query 4: Sorted output ####
# Sort output based on the volcanos which killed the most people
print("\nShowing sorted output from most deaths to least: ")

volcanoCursor = eruptions.find().sort("Deaths", -1) # Sorted in descending order
for erup in volcanoCursor:
    pprint(erup)

#### Query 5: Aggregation ####
# Goal: Get the sum of all deaths caused by volcanoes
print("\nShowing aggregation by getting the sum of all deaths: ")

pipe = [{'$group': {'_id': None, 'total_deaths': {'$sum': '$Deaths'}}}]
pprint(list(eruptions.aggregate(pipe)))

print("\nShowing aggregation through getting sum of all deaths each year: ")
for doc in eruptions.aggregate([{"$match": {}}, {"$group": {"_id": "$Year", "total": {"$sum": "$Deaths"} }}]):
    pprint(doc)

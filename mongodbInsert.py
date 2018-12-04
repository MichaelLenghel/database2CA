import pymongo as db
from pprint import pprint

myclient = db.MongoClient("mongodb://localhost:27017/")
# testdb is what ever youcalled your db with the use word
mydb = myclient["testdb"]

halloweenHorror = mydb["Halloween"]
chistmasEvent = mydb["Christmas"]
easter_huntEvent = mydb["Easter Hunt"]
skatingEvent = mydb["Skating"]
lineDancingEvent = mydb["LineDancing"]

halloweenHorror.drop()
halloween = [
				{"name" : "Kay Casey", "Email": "kcasey@i.ie", "cost": "20 per person"},
				{"name" : "Harry Skater", "Email": "skater@g.ie", "cost": "20 per person"}
			]


chistmasEvent.drop()
chistmas = {
			"name" : "Kay Casey", 
			"Email": "kcasey@i.ie",
			"cost": "50 per person",
			"Age Range": "Adult"
		   }

easter_huntEvent.drop()
easter_hunt = {
				"cost": "20 per person",
			  }

skatingEvent.drop()
skating = {
			"name" : "Harry Skater", 
			"Email": "hskater@g.ie", 
			"cost": "50 per person", 
			"shoeSize": "42"
		  }

lineDancingEvent.drop()
lineDancing = [
				{"name" : "Harry Skater", "Email": "hskater@g.ie", "cost": "30 per person / 50 per couple", "shoe size": "42", "gender": "Male"},
				{"name" : "Patrick Skater", "Email": "pskater@g.ie","cost": "30 per person / 50 per couple", "shoe size": "44", "gender": "Male" }
			  ]


# Insert all the tables above. (The ones in arrays done as insert_many)
x = halloweenHorror.insert_many(halloween)
x = lineDancingEvent.insert_many(lineDancing)
x = chistmasEvent.insert_one(chistmas)
x = easter_huntEvent.insert_one(easter_hunt)
x = skatingEvent.insert_one(skating)

cursor = halloweenHorror.find()
for doc in cursor:
	pprint(doc)


# This is how u do a mongodb insert:
# db.Employee.insert
# 	(
# 		{
# 			"Employeeid" : 1,
# 			"EmployeeName" : "Martin"
# 		}
# 	)
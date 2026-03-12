from pymongo import MongoClient

# MongoDB connection setup
client = MongoClient("mongodb://localhost:27017")  # Use your MongoDB URI here if needed
db = client['COM517']  # Specify the database name

# Define collections
comments_collection = db['comments']  # Collection for Comments 
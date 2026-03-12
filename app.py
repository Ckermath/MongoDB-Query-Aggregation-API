from flask import Flask, request
from pymongo import MongoClient
from routes.comments import comments_blueprint
import pandas as pd
import json
from bson import ObjectId


app = Flask(__name__)

client = MongoClient("mongodb://localhost:27017")
db = client.COM517 # Create COM517 database
comments_collection = db.comments # Create the comments collection
reviewers_collection = db.reviewers # Create the reviewers collection


app.register_blueprint(comments_blueprint, url_prefix='/api/comments')

if __name__ == '__main__':   
    app.run(debug=True, port=5000)
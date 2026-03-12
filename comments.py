from flask import Blueprint, jsonify, request
from bson import ObjectId
from db import comments_collection  # Import comments collection from db.py
from pymongo import MongoClient

# Define the Blueprint
comments_blueprint = Blueprint('comments', __name__)

client = MongoClient("mongodb://localhost:27017")
db = client.COM517
comments_collection = db.comments

@comments_blueprint.route('/view', methods=['GET'])
def select_necessary_fields():
    # Retrieve only specific fields from the collection.
    try:
        # Select specific fields but exclude _id
        results = comments_collection.find({}, {
            "Category": 1, 
            "Comment.Text": 1,
            "Comment.Type": 1, 
            "Comment.Actionable Suggestions": 1,
            "Comment Date": 1, 
            "Relevance": 1,  
            "Priority": 1,
            "_id": 0,
        })
        
        # return the results with a status code 200 if the record retrieval was successful or status code 500 if it was not 
        records = [record for record in results]
        return jsonify(records), 200
    except Exception as e:
        return jsonify({"error": f"Failed to retrieve records: {str(e)}"}), 500

@comments_blueprint.route('/view_by_tags', methods=['GET'])
def match_array_values():
    #Retrieve comments matching specific values in the Tags array.
    try:
        # Get tags from the query parameter
        tags = request.args.getlist('tag')  
        if not tags:
            return jsonify({"error": "Please provide at least one comment tag."}), 400
        # Query to match documents where Tags contain any of the specified values
        results = comments_collection.find({"Tags": {"$in": tags}}, {
            "Category": 1,
            "Comment.Text": 1,
            "Comment.Type": 1,
            "Comment.Actionable Suggestions": 1,
            "Tags": 1,
            "_id": 0
        })

        # Convert results to a list
        records = [record for record in results]

        return jsonify(records), 200
    except Exception as e:
        return jsonify({"error": f"Failed to retrieve records: {str(e)}"}), 500
    
@comments_blueprint.route('/view_by_multiple_criteria', methods=['GET'])
def match_array_multiple_criteria():
    #Retrieve comments with Issues matching multiple criteria.
    try:
        severity = request.args.get('severity')
        resolved = request.args.get('resolved')
        if not severity:
            return jsonify({"error": "Please provide a severity parameter"}), 400
        elif not resolved:
            return jsonify({"error": "Please provide a resolved parameter"}), 400
        
        resolved = resolved.lower() == 'true'  # Converts true to True and false to False boolean values
        # Query to match documents where Issues array elements satisfy multiple conditions using $elemMatch
        results = comments_collection.find({
            "Issues": {"$elemMatch": {
                    "$and": [
                        {"Severity": {"$regex": f"^{severity}$", "$options": "i"}}, # Where the serverity and resolved is entered by the user and returned from the issues array
                        {"Resolved": resolved}
                    ]
                }
            }
        }, {
            "Category": 1,
            "Comment.Text": 1,
            "Comment.Type": 1,
            "Comment.Actionable Suggestions": 1,
            "Issues": {"$elemMatch": {
                "$and": [
                    {"Severity": {"$regex": f"^{severity}$", "$options": "i"}}, # Regular expression used to ensure valid data matches the data in the database
                    {"Resolved": resolved}]}}, 
            "_id": 0
        })

        records = [record for record in results]

        if not records:
            return jsonify({"error": "No comments found for the specified criteria."}), 404
        else:
            return jsonify(records), 200

    except Exception as e:
        return jsonify({"error": f"Failed to retrieve records: {str(e)}"}), 500

@comments_blueprint.route('/view_by_specific_criteria', methods=['GET'])
def meet_specific_criteria(): # Query that matches elements from the issues array based on the specific criteria: severity, resolved and description
    try:
        # Retrieve query parameters
        severity = request.args.get('severity')
        resolved = request.args.get('resolved')
        query = request.args.get('query')

        # Validate user input
        if not severity:
            return jsonify({"error": "Please provide a severity parameter"}), 400
        if not resolved:
            return jsonify({"error": "Please provide a resolved parameter"}), 400
        if not query:
            return jsonify({"error": "Please provide a query parameter"}), 400
        
        resolved = resolved.lower() == 'true'

        results = comments_collection.find({
            "Issues": {
                "$elemMatch": {
                    "Severity": {"$regex": f"^{severity}$", "$options": "i"}, # Where the serverity and resolved is entered by the user and returned from the issues array
                    "Resolved": resolved,                  
                    "$or": [
                        {"Description": {"$regex": query, "$options": "i"}}  # Matching the entered query to data in the issue description
                    ]
                }
            }
        }, {
            "Category": 1,                           
            "Comment.Text": 1,                        
            "Comment.Type": 1,
            "Comment.Actionable Suggestions": 1,                        
            "Issues": 1,                            
            "_id": 0                                  
        })
        
        records = [record for record in results]

        # If no records are found, return an error message
        if not records:
            return jsonify({"error": "No comments found with the specific criteria."}), 404

        return jsonify(records), 200

    except Exception as e:
        return jsonify({"error": f"Failed to retrieve records: {str(e)}"}), 500
    
@comments_blueprint.route('/print_results', methods=['GET'])
def print_results(): # Iterate and print the documents returned by the query 
    try:
        severity = request.args.get('severity')
        resolved = request.args.get('resolved')
        if not severity:
            return jsonify({"error": "Please provide a severity parameter"}), 400
        if not resolved:
            return jsonify({"error": "Please provide a resolved parameter"}), 400
        
        resolved = resolved.lower() == 'true'

        # Retrieve documents with specific criteria
        results = comments_collection.find({
            "Issues": {
                "$elemMatch": {
                    "Severity": {"$regex": f"^{severity}$", "$options": "i"},
                    "Resolved": resolved
                }
            }
        }, {
            "Category": 1,
            "Comment.Text": 1,
            "Issues": 1,
            "_id": 0
        })
        records = list(results)
        if not records:
            return jsonify({"error": "No matching documents found."}), 404
        for doc in records:
            print(doc) # output the document
        return jsonify({"message": "Documents printed successfully."}), 200

    except Exception as e:
        return jsonify({"error": f"Failed to retrieve and print records: {str(e)}"}), 500
    
@comments_blueprint.route('/query_embedded', methods=['GET'])
def query_embedded_documents():
    #Retrieve comments where embedded Issues meet specific criteria.
    try:
        # Get query parameters
        severity = request.args.get('severity')  # High, Medium, or Low
        description_query = request.args.get('description')  # Description match

        # Validate user input
        if not severity:
            return jsonify({"error": "Please provide a severity parameter."}), 400
        if not description_query:
            return jsonify({"error": "Please provide a description query parameter."}), 400

        results = comments_collection.find({
            "Issues": {
                "$elemMatch": {
                    "Severity": {"$regex": f"^{severity}$", "$options": "i"},  
                    "Description": {"$regex": description_query, "$options": "i"} 
                }
            }
        }, {
            "Category": 1,
            "Comment.Text": 1,
            "Comment.Type": 1,
            "Issues.$": 1,  # Only include the matching array element
            "_id": 0
        })

        records = [record for record in results]

        if not records:
            return jsonify({"error": "No matching comments found."}), 404

        return jsonify(records), 200

    except Exception as e:
        return jsonify({"error": f"Failed to retrieve records: {str(e)}"}), 500
    
@comments_blueprint.route('/match', methods=['GET'])
def match_tags_and_issues():
    #Match documents based on Tags array and Issues array with multiple criteria.
    try:
        tags = request.args.getlist('tag')  # Accept multiple tags
        severity = request.args.get('severity')  
        resolved = request.args.get('resolved')  
        description_query = request.args.get('description')  
        
        if not tags:
            return jsonify({"error": "Please enter a valid tag."}), 400
        if not severity:
            return jsonify({"error": "Please enter at least one severity."}), 400
        if not resolved:
            return jsonify({"error": "Please enter a resolved parameter."}), 400
        if not description_query:
            return jsonify({"error": "Please provide a description query."}), 400
        
        resolved = resolved.lower() == 'true'

        query = {
            "$and": [
                {"Tags": {"$in": tags}},  # Match documents where Tags contain at least one of the specified values
                {"Issues": {
                    "$elemMatch": {
                        "$and": [
                            {"Severity": {"$regex": f"^{severity}$", "$options": "i"}},  
                            {"Resolved": resolved},  
                            {"Description": {"$regex": description_query, "$options": "i"}} 
                        ]
                    }
                }}
            ]
        }
        results = comments_collection.find(query, {
            "Category": 1,
            "Comment.Text": 1,
            "Tags": 1,
            "Issues": 1,  
            "_id": 0
        })
        records = [record for record in results]
        # Validation if no records are found
        if not records:
            return jsonify({"error": "No matching documents found."}), 404
        return jsonify(records), 200
    except Exception as e:
        return jsonify({"error": f"Failed to retrieve records: {str(e)}"}), 500

@comments_blueprint.route('/match_tags_all', methods=['GET'])
def match_tags_with_all_elements():
    #Match documents where the Tags array contains all specified elements.
    
    try:
        tags = request.args.getlist('tag')  
        if not tags:
            return jsonify({"error": "Please provide at least one tag to match."}), 400

        # Query to match documents where Tags array contains all specified elements
        query = {"Tags": {"$all": tags}}

        results = comments_collection.find(query, {
            "Category": 1,
            "Comment.Text": 1,
            "Tags": 1,
            "_id": 0
        })

        records = [record for record in results]

        if not records:
            return jsonify({"error": "No matching documents found."}), 404
        return jsonify(records), 200

    except Exception as e:
        return jsonify({"error": f"Failed to retrieve records: {str(e)}"}), 500

@comments_blueprint.route('/text_search', methods=['GET'])
def text_search():
    # Perform a text search on the Comment.Text field.
    try:
        search_query = request.args.get('query')  # Get the search query
        
        if not search_query:
            return jsonify({"error": "Please provide a search query."}), 400
        
        # Perform text search 
        results = comments_collection.find(
            {"$text": {"$search": search_query}},
            {
                "Category": 1,
                "Comment.Text": 1,
                "Comment Date": 1,
                "Tags": 1,
                "_id": 0
            }
        )
        records = list(results)

        if not records:
            return jsonify({"error": "No matching documents found."}), 404

        return jsonify(records), 200
    except Exception as e:
        return jsonify({"error": f"Failed to perform text search: {str(e)}"}), 500

@comments_blueprint.route('/join_comments_and_reviewers', methods=['GET'])
def join_comments_and_reviewers():
    #Perform a left outer join between the comments and reviewers collections.
    try:
        # pipeline to join comments and reviewers
        pipeline = [
            {
                "$lookup": {
                    "from": "reviewers",  # The collection to join
                    "localField": "ReviewerID",  # The field from the comments collection
                    "foreignField": "_id",  # The foreign field from the reviewers collection
                    "as": "reviewer_details"  
                }
            },
            {
                "$unwind": {
                    "path": "$reviewer_details",  # Put the reviewer details into individual fields
                    "preserveNullAndEmptyArrays": True  # Keep comments incase there is no matching reviewer in the reviewers collection
                }
            },
            {
                "$project": {
                    "Category": 1,
                    "Comment.Text": 1,
                    "Comment Date": 1,
                    "reviewer_details.Name": 1, 
                    "reviewer_details.Role": 1, 
                    "reviewer_details.Email" : 1,
                    "_id": 0
                }
            }
        ]
        
        results = comments_collection.aggregate(pipeline)
        records = [record for record in results]

        if not records:
            return jsonify({"error": "No matching documents found."}), 404
        
        return jsonify(records), 200

    except Exception as e:
        return jsonify({"error": f"Failed to retrieve records: {str(e)}"}), 500

@comments_blueprint.route('/data_transform', methods=['GET'])
def data_transform():
    # Query to transform the category to uppercase and combine the reviewer's name and role 
    try:
        pipeline = [
            {
                "$lookup": {
                    "from": "reviewers",           
                    "localField": "ReviewerID",      
                    "foreignField": "_id",           
                    "as": "reviewer_details"         
                }
            },
            {
                "$unwind": {
                    "path": "$reviewer_details",      
                    "preserveNullAndEmptyArrays": True  
                }
            },
            {
                "$addFields": {
                    "Reviewer Details": {
                        "$concat": [
                            "$reviewer_details.Name", " - ", "$reviewer_details.Role" # Combine the reviewer's name and role
                        ]
                    },
                    "Category": {
                        "$toUpper": "$Category"  # Convert Category to uppercase
                    }
                }
            },
            {
                "$project": {
                    "Category": 1,                   
                    "Comment.Text": 1,
                    "Comment Date": 1,                
                    "Reviewer Details": 1,            
                    "_id": 0                          
                }
            }
        ]

        results = comments_collection.aggregate(pipeline)

        records = [record for record in results]

        if not records:
            return jsonify({"error": "No matching documents found."}), 404

        return jsonify(records), 200

    except Exception as e:
        return jsonify({"error": f"Failed to retrieve records: {str(e)}"}), 500

@comments_blueprint.route('/deconstruct_array', methods=['GET'])
def deconstruct_array():
    try:
        pipeline = [
            {
                "$unwind": {
                    "path": "$Issues",                   # Deconstruct Issues array
                    "includeArrayIndex": "issue_index"   # Code to include the index of each element
                }
            },
            {
                "$project": {
                    "Category": 1,                      
                    "Comment.Text": 1,                 
                    "Issues": 1,                        
                    "issue_index": 1,                   
                    "_id": 0                            
                }
            }
        ]

        results = comments_collection.aggregate(pipeline)

        records = [record for record in results]

        if not records:
            return jsonify({"error": "No matching documents found."}), 404

        return jsonify(records), 200

    except Exception as e:
        return jsonify({"error": f"Failed to retrieve records: {str(e)}"}), 500

@comments_blueprint.route('/word_count', methods=['GET'])
def word_count():
    #Count the occurrences of each word in Comment.Text across all comments
    try:
        pipeline = [
            # Step 1: Mapping Phase - Split the Comment.Text field into words
            {
                "$project": {
                    "words": {
                        "$split": ["$Comment.Text", " "]  # Split the text into words based on space
                    }
                }
            },
            # Step 2: Filtering Phase - Remove empty words
            {
                "$unwind": "$words"  # Creates a separate documents for each word 
            },
            {
                "$match": {
                    "words": {"$ne": ""}  # Exclude empty strings from the words
                }
            },
            # Step 3: Reducing Phase - Group words and count their occurrences
            {
                "$group": {
                    "_id": "$words",  # Group by the word itself
                    "count": {"$sum": 1}  # Count the occurrences of each word
                }
            },
            # Step 4: Sorting - Sort the words by frequency in descending order
            {
                "$sort": {
                    "count": -1  
                }
            }
        ]

        results = comments_collection.aggregate(pipeline)
        records = [{"word": doc["_id"], "count": doc["count"]} for doc in results]

        if not records:
            return jsonify({"error": "No words found."}), 404

        return jsonify(records), 200

    except Exception as e:
        return jsonify({"error": f"Failed to retrieve records: {str(e)}"}), 500

@comments_blueprint.route('/aggregate_comments_by_priority', methods=['GET'])
def aggregate_comments_by_priority():
    try:
        pipeline = [
            # Group by the priority and count the number of comments per priority level (High, Medium or Low)
            {
                "$group": {
                    "_id": "$Priority",  
                    "comment_count": {"$sum": 1}
                }
            },
            # Use conditional expressions to categorise by priority
            {
                "$project": {
                    "_id": 1,
                    "comment_count": 1,
                    "priority_category": {
                        "$cond": {
                            "if": {"$eq": ["$_id", "High"]},  # If priority is High, categorise as a 'High Priority'
                            "then": "High Priority",
                            "else": {
                                "$cond": {
                                    "if": {"$eq": ["$_id", "Medium"]},  # If priority is Medium, categorise as a 'Medium Priority'
                                    "then": "Medium Priority",
                                    "else": "Low Priority"  # Else, if categorise as 'Low Priority'
                                }
                            }
                        }
                    }
                }
            },
            # Comments are sorted by the count of comments in descending order
            {
                "$sort": {"comment_count": -1}
            }
        ]

        results = comments_collection.aggregate(pipeline)
        records = [record for record in results]

        if not records:
            return jsonify({"error": "No matching documents found."}), 404
        return jsonify(records), 200
    except Exception as e:
        return jsonify({"error": f"Failed to retrieve records: {str(e)}"}), 500

@comments_blueprint.route('/comment_resolved_issue', methods=['POST'])
def update_resolved_issue():
    try:
        search_query = request.args.get('query')  # Get the search query
        if not search_query:
            return jsonify({"error": "Please provide enter the issue that has been resolved."}), 400

        result = comments_collection.update_many(
            {
                "Issues.Description": {"$regex": search_query, "$options": "i"},  # Looking for issues with the user-provided description (not case-sensitive)
                "Issues.Resolved": False  # Only unresolved issues
            },
            {
                "$set": {
                    "Issues.$[elem].Resolved": True  # Set the specific issue as resolved
                }
            },
            array_filters=[{ # Narrows the update to a specific issue within the issues array
                "elem.Description": {"$regex": search_query, "$options": "i"},
                "elem.Resolved": False
            }]
        )

        if result.modified_count > 0:
            return jsonify({"message": "Documents updated successfully."}), 200
        else:
            return jsonify({"message": "No documents matched the criteria to update."}), 404

    except Exception as e:
        return jsonify({"error": f"Failed to perform the update: {str(e)}"}), 500
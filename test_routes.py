import pytest
from flask import Flask
from bson import ObjectId
from pymongo import MongoClient
from routes.comments import comments_blueprint

client = MongoClient("mongodb://localhost:27017")
db = client.COM517
comments_collection = db.comments


@pytest.fixture
def test_client():
    app = Flask(__name__)
    app.register_blueprint(comments_blueprint, url_prefix = '/api/comments')
    app.testing = True 
    with app.test_client() as client:
        with app.app_context(): # Create and push an app context
            yield client

def test_match_array_elements_multiple_criteria(test_client):
    # Simulate a valid request with severity and resolved
    response = test_client.get("/api/comments/view_by_multiple_criteria", 
                               query_string={"severity": "high", "resolved": "true"})
    assert response.status_code == 200  
    data = response.json
    assert len(data) > 0  
    for record in data:
        issues = record.get("Issues", [])
        for issue in issues:
            assert issue.get("Severity").lower() == "high"
            assert issue.get("Resolved") is True

def test_case_insensitive_severity(test_client):
    # Test case insensitivity by providing severity as "High" instead of "high"
    response = test_client.get("/api/comments/view_by_multiple_criteria", query_string={"severity": "High", "resolved": "true"})
    assert response.status_code == 200
    data = response.json
    assert len(data) > 0
    for comment in data:
        issues = comment.get("Issues", [])
        assert any(issue["Severity"].lower() == "high" and issue["Resolved"] is True for issue in issues)

def test_empty_severity(test_client):
    # Test when the severity parameter is empty
    response = test_client.get("/api/comments/view_by_multiple_criteria", query_string={"severity": "", "resolved": "true"})
    assert response.status_code == 400
    assert response.json['error'] == "Please provide a severity parameter"

def test_empty_resolved(test_client):
    # Test when the resolved parameter is empty
    response = test_client.get("/api/comments/view_by_multiple_criteria", query_string={"severity": "high", "resolved": ""})
    assert response.status_code == 400
    assert response.json['error'] == "Please provide a resolved parameter"

def test_match_arrays_containing_all_specified_elements(test_client):
    # Test that checks all tags are present in the Tags array
    response = test_client.get("/api/comments/match_tags_all", query_string={"tag": ["title page", "structure"]})
    
    assert response.status_code == 200  
    data = response.json
    assert len(data) > 0 
    
    for record in data:
        tags = record.get("Tags", [])
        # Check if all the specified tags are in the 'Tags' field
        for tag in ["title page", "formatting"]:
            assert tag in tags

def test_no_matching_tags(test_client):
    # Test case where no documents match the specified tags
    response = test_client.get("/api/comments/match_tags_all", query_string={"tag": ["no", "tags"]})
    
    # Check for a 404 status code and error message
    assert response.status_code == 404
    assert response.json['error'] == "No matching documents found."

def test_left_outer_join_comments_and_reviewers(test_client):
    # Test that verifies a successful left outer join between comments and reviewers collections
    response = test_client.get("/api/comments/join_comments_and_reviewers")

    # Check if the response status code is 200 for a successful join operation
    assert response.status_code == 200
    data = response.json
    assert len(data) > 0 
    
    # Check if the response contains correct fields from both collections
    for record in data:
        assert "Category" in record
        assert "Text" in record["Comment"]
        assert "Comment Date" in record
        assert "reviewer_details" in record
        assert "Name" in record["reviewer_details"]
        assert "Role" in record["reviewer_details"]
        assert "Email" in record["reviewer_details"]

def test_left_outer_join_no_match(test_client):
    # Test the case where no matching reviewer is found in the reviewers collection
    response = test_client.get("/api/comments/join_comments_and_reviewers")
    
    assert response.status_code == 200
    data = response.json
    assert len(data) > 0  
    
    # Ensure that for records with no matching reviewer, reviewer details are empty or null
    for record in data:
        if not record.get("reviewer_details"):
            assert record["reviewer_details"] == {}
        else:
            assert "Name" in record["reviewer_details"]
            assert "Role" in record["reviewer_details"]
            assert "Email" in record["reviewer_details"]

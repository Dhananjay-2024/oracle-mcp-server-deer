"""Test the DQ validation endpoint"""
import requests
import json

# Test configuration
BASE_URL = "http://localhost:8000"
DATABASE_NAME = "default"  # Replace with your actual database name

# Sample DQ rule
sample_rule = {
    "rule_id": "test_mandatory_id",
    "rule_type": "mandatory",
    "category": "data_validity",
    "target_columns": ["EMPLOYEE_ID"],  # Replace with actual column
    "params": {},
    "enabled": True
}

# Request body
request_body = {
    "table_name": "EMPLOYEES",  # Replace with your actual table
    "rules": [sample_rule],
    "store_results": True,
    "sample_percent": None,
    "sample_failed_rows": 5
}

# Make the request
url = f"{BASE_URL}/sql/{DATABASE_NAME}/dq-rules/validate"
print(f"Calling: {url}")
print(f"Body: {json.dumps(request_body, indent=2)}")

try:
    response = requests.post(url, json=request_body)
    print(f"\nStatus Code: {response.status_code}")
    print(f"Response: {json.dumps(response.json(), indent=2)}")
except Exception as e:
    print(f"Error: {e}")
    if hasattr(e, 'response'):
        print(f"Response text: {e.response.text}")

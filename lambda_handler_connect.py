import boto3
import json
import os
import logging
import traceback
import urllib.request

logger = logging.getLogger()
logger.setLevel(logging.INFO)
dynamodb = boto3.resource('dynamodb')

def response_proxy(data):
    '''
    For HTTP status codes, you can take a look at https://httpstatuses.com/
    '''
    try:
        response = {}
        response["isBase64Encoded"] = False
        response["statusCode"] = data["statusCode"]
        response["headers"] = {}
        if "headers" in data:
            response["headers"] = data["headers"]
        response["body"] = json.dumps(data["body"])
        return response
    except Exception:
        logger.info(traceback.format_exc())
        return {}


def prep_response(status_code, message):
    '''
    Function template for quick return response
    '''
    data = {}
    data["statusCode"] = status_code
    data["headers"] = {}
    data["body"] = {}
    data["body"]["message"] = message
    return response_proxy(data)


def lambda_handler(event, context):

    token = None
    
    # Temporary Code to Deactivate Login
    saveConnection(event)
    return prep_response(200, "OK")

    # 1. Check for 'token' in query string parameters first
    query_params = event.get('queryStringParameters')
    if query_params and 'token' in query_params:
        token = query_params['token']
        print("Token found in query string parameters.")
    
    # 2. If not found, check the Authorization header
    else:
        auth_header = event['headers'].get('Authorization') or event['headers'].get('authorization')
        if auth_header and auth_header.startswith('Bearer '):
            print("Token found in Authorization header.")
            # Extract the token from the "Bearer <token>" string
            token = auth_header.split(' ')[1]
        

    # 3. If no token is found in either place, return an error
    if not token:
        print("Unauthorized: Token missing from both query parameters and Authorization header.")
        return prep_response(401, 'Unauthorized: Missing or invalid token.')
    # else:
    #     return prep_response(200, "OK")


    # auth_header = event['headers'].get('Authorization') or event['headers'].get('authorization')
    # if not auth_header or not auth_header.startswith('Bearer '):
    #     return {
    #         'statusCode': 401,
    #         'body': 'Unauthorized: Missing or invalid Authorization header'
    #     }

    # token = auth_header#.split(' ')[1]
    url = os.getenv("VALIDATION_URL")#"https://hull-api-stag.synergymarine.in/api/oec/Home/Health-Auth-Dev"
    # Create a request with the Authorization header
    req = urllib.request.Request(
        url,
        headers={
            "Authorization": "Bearer "+token
        },
        method="GET"
    )
    
    

    # Make the request
    try:
        with urllib.request.urlopen(req, timeout=60) as response:
            status_code = response.getcode()
            body = response.read().decode()
            print("Response Status Code:", status_code)
            print("Response Body:", body)
            saveConnection(event)
            return prep_response(200, "OK")
    except urllib.error.HTTPError as e:
        print(f"HTTP Error: {e.code} - {e.reason}")
        return prep_response(e.code, e.reason)
    except urllib.error.URLError as e:
        print(f"URL Error: {e.reason}")
        return prep_response(e.code, e.reason)
    


def saveConnection(event):
    try:
        connection_id = event['requestContext']['connectionId']
        api_id = event['requestContext']['apiId']
        connected_timestamp = event['requestContext']['connectedAt']
        print(connection_id, api_id, connected_timestamp)
        if connection_id and api_id and connected_timestamp:
            print("Inside If0")
            
            print("Inside If1")
            table = dynamodb.Table(os.getenv("TABLE_NAME"))
            print("Inside If2")
            table.put_item(
                Item={
                    'connectionId': connection_id,
                    'apiId': api_id,
                    'connectedTimestamp': connected_timestamp
                }
            )
            print("Added Item to DB...")
            return True
    except Exception as e:
        print(e)
        logger.info(e)
        logger.error(e)
        return False
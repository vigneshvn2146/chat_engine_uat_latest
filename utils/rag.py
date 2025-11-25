import boto3
from botocore.exceptions import NoCredentialsError
import os
import re
from vespa.application import Vespa
from vespa.io import VespaQueryResponse
import pandas as pd
import json
import hashlib
from flask import Flask, request, jsonify
import base64

# Paths and endpoint
# global cert_base64
# global key_base64
global endpoint

# cert_path = """LS0tLS1CRUdJTiBDRVJUSUZJQ0FURS0tLS0tCk1JSUJOekNCM3FBREFnRUNBaEFzdzhtclVuU3hwODA2RVI3YXA3NWpNQW9HQ0NxR1NNNDlCQU1DTUI0eEhEQWEKQmdOVkJBTVRFMk5zYjNWa0xuWmxjM0JoTG1WNFlXMXdiR1V3SGhjTk1qVXdNekF6TURrd05qRXdXaGNOTXpVdwpNekF4TURrd05qRXdXakFlTVJ3d0dnWURWUVFERXhOamJHOTFaQzUyWlhOd1lTNWxlR0Z0Y0d4bE1Ga3dFd1lICktvWkl6ajBDQVFZSUtvWkl6ajBEQVFjRFFnQUVkM1FOckhSWVVGYytYWEFZQ2ZpUC90NmRKZjJJbVBNaitadkQKL1V4cjkvbUhiWWFQVHZHdFVCRzJPZ09Obkh1OHVMVVdqc2ZqN0VwK255UFRtTE1kZ3pBS0JnZ3Foa2pPUFFRRApBZ05JQURCRkFpRUE2cW9aY1BqaG1YbC9tZkZqSGpZUy96VGJpbDFkVEs3cnB3YXl0bDNtK0pBQ0lCdWR0aGJQCkVkSW9LTDJidSs0V0cvZWpGTHFYMGYrcDMrcFc5Z2RnOFBGSAotLS0tLUVORCBDRVJUSUZJQ0FURS0tLS0tCg=="""
# key_path = """LS0tLS1CRUdJTiBQUklWQVRFIEtFWS0tLS0tCk1JR0hBZ0VBTUJNR0J5cUdTTTQ5QWdFR0NDcUdTTTQ5QXdFSEJHMHdhd0lCQVFRZ3FTWGM2N1lVU1NnbGNEOWEKSkRPUlhwVU1kUHI4c1Vud0kxNmVXUWUvSWZ1aFJBTkNBQVIzZEEyc2RGaFFWejVkY0JnSitJLyszcDBsL1lpWQo4eVA1bThQOVRHdjMrWWR0aG85TzhhMVFFYlk2QTQyY2U3eTR0UmFPeCtQc1NuNmZJOU9Zc3gyRAotLS0tLUVORCBQUklWQVRFIEtFWS0tLS0tCg=="""


# cert_base64 = cert_path
# key_base64 = key_path
endpoint = "https://fd4e8b47.eae6f79b.z.vespa-app.cloud/"

# AWS Bedrock Client
aws_access_key = os.getenv('aws_access_key_id')
aws_secret_key = os.getenv('aws_secret_access_key')
aws_region = 'ap-south-1'

# Decode certificates
# cert_path = base64.b64decode(cert_base64)
# key_path = base64.b64decode(key_base64)

# Save certificates as temporary files
cert_file_path = "./utils/data-plane-public-cert.pem"
key_file_path = "./utils/data-plane-private-key.pem"

# with open(cert_file_path, "wb") as cert_file:
#     cert_file.write(cert_path)

# with open(key_file_path, "wb") as key_file:
#     key_file.write(key_path)

# Initialize Vespa app session
the_app = Vespa(endpoint, cert=cert_file_path, key=key_file_path)

# Flask app for webhook (Optional, if you plan to expose an API)
app = Flask(__name__)

bedrock_runtime = boto3.client(
    service_name="bedrock-runtime",
    aws_access_key_id=aws_access_key,
    aws_secret_access_key=aws_secret_key,
    region_name=aws_region
)

# Function to get embeddings
def get_embeddings(query):
    body = json.dumps({"inputText": query})
    model_id = "amazon.titan-embed-image-v1"

    response = bedrock_runtime.invoke_model(
        body=body,
        modelId=model_id,
        accept="application/json",
        contentType="application/json"
    )

    response_body = json.loads(response["body"].read())
    return response_body.get("embedding")

# Function to generate text using Bedrock (Llama 3)
def generate(context, model_id="meta.llama3-8b-instruct-v1:0"):
    formatted_prompt = f"""
    <|begin_of_text|><|start_header_id|>user<|end_header_id|>
    {context}
    <|eot_id|>
    <|start_header_id|>assistant<|end_header_id|>
    """

    request_payload = json.dumps({"prompt": formatted_prompt, "max_gen_len": 1024, "temperature": 0.5})

    response = bedrock_runtime.invoke_model(modelId=model_id, body=request_payload)
    model_response = json.loads(response["body"].read())

    return model_response["generation"]

# Function to get unique ID
def get_unique_id(content):
    return hashlib.sha256(content.encode()).hexdigest()

# Function to display hits as DataFrame
def display_hits_as_df(response, fields):
    records = []
    for hit in response.hits:
        record = {field: hit["fields"].get(field, None) for field in fields}
        records.append(record)
    return pd.DataFrame(records)

# Function to query and rerank results
def query_rerank(session, query, ranking="fusion"):
    response = session.query(
        yql="""select id, page_title, topic, equipment, page_number, person, vessel, engine_model, engine_make, source_url, url, info_type, document_type 
        from doc where ({targetHits: 100, approximate: true}nearestNeighbor(embedding, q1024)) limit 30""",
        ranking=ranking,
        timeout=10,
        body={"presentation.timing": "true", "input.query(q1024)": query["embedding"]}
    )

    if hasattr(response, "hits") and response.hits:
        print("Hits found")
    else:
        print("No hits found")

    return display_hits_as_df(response, ["page_title", "topic", "equipment", "page_number", "person", "vessel", "engine_model", "engine_make", "source_url", "url", "info_type", "document_type"])

# Function to generate the prompt
# def get_prompt(u_query, Ucontext):
#     prompt = f"""
#     Context: You are tasked with generating a detailed answer for a user query. Remove the reference from your response. The user query is: {u_query}.
#     You're also provided with the top 20 relevant contextual information:
#     {Ucontext}

#     Return as a markdown file that is easy to read.

#     NOTE:
#     1) Ensure the response is well-structured and formatted as a Markdown file.
#     2) If unable to generate a response, inform the user gracefully. Don't come up with answer from your own knowledge.
#     """
#     return prompt

def get_prompt(u_query, Ucontext):
    prompt = f"""You're a Maritime Expert bot. Your task is to answer the user queries by reasoning and understanding from the given context:
    
    User Question: {u_query}
    
    Context Information:
    {Ucontext}
    
    Response Instructions:
    - Format your response step by step as a markdown document
    - Try to answer specifically to the user question only. Don't provide any other information other than the information requested by the user.
    - Strictly Markdown structured format
    - Base your answer only on the provided context information if you feel that there is insufficient information to answer the query gracefully acknowledge you're not able to answer the question.
    - If the information is insufficient, acknowledge the limitations
    - Include the sources along with the page number it got refered from.
    """
     # - Do not cite sources / references directly within the response

    return prompt

# Function to format Markdown response
def get_markdown(res):
    prompt = "Format the markdown file to be visually appealing and easy to read."
    context = "Response: " + res
    result = generate(f"Markdown formatted response:\n\n{prompt}\n\n{context}")
    return result

def main(query_input, vessel='ace eternity', make='MAN Diesel A/S', model=''):
    print(f"query input : {query_input}")
    embedding = get_embeddings(query_input)
    unique_id = get_unique_id(query_input)
    query = {"text": query_input, "embedding": embedding, "id": unique_id}

    global endpoint
    global cert_path
    global key_path

    cert_path = './utils/data-plane-public-cert.pem'
    key_path = './utils/data-plane-private-key.pem'

    print(endpoint, cert_path, key_path)
    
    # response = query_rerank(query, vessel, make=make, model=model, ranking="fusion")
    # response = query_rerank(query)
    the_app = Vespa(endpoint, cert=cert_path, key=key_path)
    
    with the_app.syncio() as session:
        response = query_rerank(session, query, ranking="fusion")

    if not response.empty:
        u_query = query_input
        print(f"\nRAG : user query : {u_query}")
        u_context = response.to_string(index=False)
        sysprompt = get_prompt(u_query, u_context)
        # result = generate(prompt)
        print(f"\nRAG prompt : {sysprompt}")
        result = generate(sysprompt)
        print(f"\nRAG response : {result}")
        # markdown = get_markdown(result)
        return result
    else:
        return "No results found."

if __name__ == "__main__":
    query_input = input("Enter your query: ")
    # print(main(query_input, vessel='ace eternity', make='MAN BW', model='7S65::8.5'))
    print(main(query_input))
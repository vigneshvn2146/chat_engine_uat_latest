from langchain_openai import AzureChatOpenAI
from langchain.prompts import PromptTemplate
from langchain.chains import LLMChain

# Azure OpenAI Config
llm = AzureChatOpenAI(
    azure_endpoint='https://openai-synergy-useast-demo.openai.azure.com/',
    api_key='893a702d43ab4a28b725ed558fc675e2',
    azure_deployment="gpt-4o-2024-11-20",
    api_version="2024-08-01-preview",
    temperature=0,
    max_tokens=256,
    timeout=10,
    max_retries=2,
)

sample_yql_query = """select id, title, equipment, series_no, content, pageno, person, vessel, model, created_date, source, make, url, detail, markorspec, enginetype, doctype
               from doc where ({targetHits: 100, approximate: true}nearestNeighbor(embedding, q1024)) limit 10"""

# Define a Prompt Template
prompt = PromptTemplate(input_variables=["query"],  # Define what input variables the template expects
    

    template="""
    Extract the "make" and "model" from the following json and generate the vespa query for the same and the make and model should be Nearest Neighbour based:
    Sample YQL Query : {sample_yql_query}
    Query: {query}

    Respond in JSON format with keys "make" and "model".
    """
)

# Create an LLM Chain
chain = LLMChain(llm=llm, prompt=prompt)

# Example User Query
user_query = "Tell me the specifications of the Toyota Corolla 2022."

# Run the Chain
response = chain.run(user_query)

# Print the Extracted Make and Model
print("Extracted Response:", response)
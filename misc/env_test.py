from dotenv import find_dotenv, load_dotenv
import os
import json

load_dotenv(find_dotenv())


var = os.environ.get('SERVICE_ACCT_JSON')
j = json.loads(var)

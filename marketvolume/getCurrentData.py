from genericpath import exists
import requests
from textwrap import indent
from datetime import datetime
import pandas as pd
import yaml
import json
import pymysql
import csv
import os

# Variables
region = "eu"
realm = "eredar"
realmId = "3692"

#########################
## Get Data From Blizz ##
#########################

# Client API data
with open("secretData.yaml", "r") as yamlfile:
    secretData = yaml.safe_load(yamlfile)
    # secretData['client_id']
    # secretData['client_secret']

# Create a new Access Token
def create_access_token(client_id, client_secret, region = 'eu'):
    data = { 'grant_type': 'client_credentials' }
    response = requests.post('https://%s.battle.net/oauth/token' % region, data=data, auth=(client_id, client_secret))
    return response.json()

response = create_access_token(secretData['client_id'], secretData['client_secret'])
token = response['access_token']
token

# Fuction to get all the auction house data for a specific realm
def get_realm_auctions(token, realmId):
    search = "https://eu.api.blizzard.com/data/wow/connected-realm/3692/auctions?namespace=dynamic-eu&locale=en_GB&access_token=" + token
    response = requests.get(search)
    return response.json()['auctions']

# Execute function and save data to a data frame
auction_df = pd.DataFrame(get_realm_auctions(token, realmId))
auction_df.head()

# Expand the item column
auction_df = auction_df.rename(columns={"id": "auction_id",})
auction_df = pd.concat([auction_df.drop(['item'], axis=1), auction_df['item'].apply(pd.Series)], axis=1)
auction_df = auction_df.rename(columns={"id": "item_id",})

# Check if region folder exists
dir = os.path.join(f"ahData/{region}")
if not os.path.exists(dir):
    os.mkdir(dir)

# Check if realm folder exists
dir = os.path.join(f"ahData/{region}/{realm}")
if not os.path.exists(dir):
    os.mkdir(dir)

# Save data to file in specific folder
filename = datetime.now().strftime(f'ahData/{region}/{realm}/%Y-%m-%d-%H-%M.csv')
auction_df.to_csv(filename, index=False)
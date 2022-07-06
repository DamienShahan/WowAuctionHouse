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

# Drop 'bonus_list' and 'modifiers' 
#   These are subgroups of an equipable item with the bonus stats (intellect agility, strength, etc)
auction_df['collection_year'] = datetime.now().strftime('%Y')
auction_df['collection_month'] = datetime.now().strftime('%m')
auction_df['collection_day'] = datetime.now().strftime('%d')
auction_df['collection_hour'] = datetime.now().strftime('%H')
auction_df['collection_datetime'] = datetime.now().strftime('%Y-%m-%d %H-%M')

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

#####################
## Save Data to DB ##
#####################

# Connect to the database
connection = pymysql.connect(host='robinhood-seo.com',
                             user=secretData['db_user'],
                             password=secretData['db_password'],
                             db=secretData['db_name'])

cursor = connection.cursor()

# Read and clean data
#ah_data = pd.read_csv('Eredar_EU-2022-07-06-15-45.csv', index_col=False, delimiter = ',')
ah_data_cleaned = auction_df.where(pd.notnull(auction_df), "NULL")

# Create a new record
tableName = f"server_{realm}_data"
sqlInsert = f"INSERT INTO `{tableName}` (`auction_id`, `quantity`, `unit_price`, `time_left`, `buyout`, `bid`, `item_id`, `context`, `bonus_lists`, `modifiers`, `pet_breed_id`, `pet_level`, `pet_quality_id`, `pet_species_id`, `collection_year`, `collection_month`, `collection_day`, `collection_hour`, `collection_datetime`) VALUES "

rowNumber = 0
# Loop through AH data
for index, row in ah_data_cleaned.iterrows():
    rowNumber = rowNumber + 1
    print(str(rowNumber)+"/"+str(len(ah_data_cleaned)))
    # Add commma if not the first row
    if rowNumber > 1:
        sqlInsert += ","

    # Start row with (
    sqlInsert += "("

    # Loop through each value
    for i in range(0,19):
        # Add comma if not the first value
        if i != 0:
            sqlInsert += f","
        
        # Certain values are strings and require quotes
        if i == 3 or i == 8 or i == 9 or i == 18:
            if row[i] == 'NULL':
                sqlInsert += f"NULL"
            else:
                sqlInsert += f"\"{row[i]}\""
        else:
            sqlInsert += f"{row[i]}"

    # Finish row with )
    sqlInsert += ")"

# Execute the query
cursor.execute(sqlInsert)

# the connection is not autocommited by default. So we must commit to save our changes.
connection.commit()
connection.close()
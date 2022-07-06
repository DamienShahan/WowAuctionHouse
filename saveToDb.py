from textwrap import indent
from matplotlib.pyplot import connect
import requests
from datetime import datetime
import pandas as pd
import yaml
import json
import pymysql
import csv

# Secret data
with open("secretData.yaml", "r") as yamlfile:
    secretData = yaml.safe_load(yamlfile)

# Connect to the database
connection = pymysql.connect(host='robinhood-seo.com',
                             user=secretData['db_user'],
                             password=secretData['db_password'],
                             db=secretData['db_name'])

cursor = connection.cursor()

# Read and clean data
ah_data = pd.read_csv('Eredar_EU-2022-07-06-15-45.csv', index_col=False, delimiter = ',')
ah_data_cleaned = ah_data.where(pd.notnull(ah_data), "NULL")

# Create a new record
sqlInsert = "INSERT INTO `server_eredar_data` (`auction_id`, `quantity`, `unit_price`, `time_left`, `buyout`, `bid`, `item_id`, `context`, `bonus_lists`, `modifiers`, `pet_breed_id`, `pet_level`, `pet_quality_id`, `pet_species_id`, `collection_year`, `collection_month`, `collection_day`, `collection_hour`, `collection_datetime`) VALUES "

rowNumber = 0
# Loop through AH data
for index, row in ah_data_cleaned.iterrows():
    rowNumber = rowNumber + 1
    print(rowNumber+"/"+len(ah_data_cleaned))
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
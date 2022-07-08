from genericpath import exists
from numpy import empty
import requests
from textwrap import indent
from datetime import datetime
import pandas as pd
import yaml
import json
import pymysql
import csv
import os
from os import listdir
from os.path import isfile, join

### Vairables ###

### Functions ###
def dataframe_difference(df1: pd.DataFrame, df2: pd.DataFrame, which=None):
    """Find rows which are different between two DataFrames."""
    comparison_df = df1.merge(
        df2,
        indicator=True,
        how='outer'
    )
    if which is None:
        diff_df = comparison_df[comparison_df['_merge'] != 'both']
    else:
        diff_df = comparison_df[comparison_df['_merge'] == which]
    return diff_df

def dataframe_difference_quantity(df1: pd.DataFrame, df2: pd.DataFrame, which=None):
    """Find rows which are different between two DataFrames."""
    comparison_df = df1.merge(
        df2,
        indicator=True,
        on='quantity',
        how='outer'
    )
    if which is None:
        diff_df = comparison_df[comparison_df['_merge'] != 'both']
    else:
        diff_df = comparison_df[comparison_df['_merge'] == which]
    return diff_df

def getCutOffAuctionId(df1, df2, item, minRightOnly):
    # Check if there is an intersecting auction_id between left_only and right_only.
    # If yes, this auction_id and all auction_ids with a greater value were new Enough.
    # All auction_ids with a lesser value were not new enough
    dataDifferences = dataframe_difference_quantity(df1[df1['item_id']==item],df2[df2['item_id']==item])
    try:
        cutOffId = getIntersectId(dataDifferences[dataDifferences['_merge']=='left_only'], dataDifferences[dataDifferences['_merge']=='right_only'])
        #print("Yes intersection")
        cutOffAuctionId = cutOffId
    except:
        #print("No intersection")
        cutOffAuctionId = minRightOnly

    return cutOffAuctionId

def getIntersectId(df1, df2):
    """Get the intersecting Id, if there is one"""
    #print(min(list(set(df1['auction_id']) & set(df2['auction_id']))))
    intersectId = min(list(set(df1['auction_id']) & set(df2['auction_id'])))

    return intersectId

### Read Files ###
regionList = os.listdir('ahData')
for region in regionList:
    serverList = os.listdir(f'ahData/{region}')

    for server in serverList:
        fileList = os.listdir(f'ahData/{region}/{server}')
        #fileList

        # Read data from 2 oldest files
        startMarketValues = pd.read_csv(f'ahData/{region}/{server}/{fileList[0]}')
        #startMarketValues[startMarketValues['item_id']==163926]
        endMarketValues = pd.read_csv(f'ahData/{region}/{server}/{fileList[1]}')
        #endMarketValues[endMarketValues['item_id']==163926]

        # Get all differences
        marketChanges = dataframe_difference(startMarketValues,endMarketValues)
        #marketChanges

        # Get a list of all unique item_id, showing which items had market volume changes
        uniqueItems = list(set(marketChanges['item_id'].unique()))
        #uniqueItems[:10]
        #uniqueItems = {163937}

        # Set market actions dataframe
        totalMarketActions = pd.DataFrame()

        # Loop through uniqueItems, generating data for each item 
        itemCounter = 0
        totalItems = len(uniqueItems)
        for item in uniqueItems:
            itemCounter = itemCounter + 1
            print(f"Item {itemCounter}/{totalItems}")
            #print(f"Item: {item}")
            # Variables
            singleMarketActions = {'item_id':item, 'amountExpired': 0, 'amountSold': 0, 'amountCanceled': 0, 'amountAdded': 0}

            itemMarketChanges = marketChanges[marketChanges['item_id'] == item]
            #itemMarketChanges

            # Only entirly sold/partially sold/expired/canceled items
            leftMarketData = itemMarketChanges[itemMarketChanges['_merge'] == 'left_only']
            #leftMarketData
            # Only amount after paritally sold or totally new auctions
            rightMarketData = itemMarketChanges[itemMarketChanges['_merge'] == 'right_only']
            #rightMarketData

            # Get the cutOff auction ID
            if rightMarketData.empty:
                minRightOnly = 0
            else:
                minRightOnly = min(rightMarketData['auction_id'])
            cutOffAuctionId = getCutOffAuctionId(startMarketValues, endMarketValues, item, minRightOnly)

            # Sold / Canceled / Expired
            for index, olderData in leftMarketData.iterrows():
                #print("-------TEST for Sold--------")
                newerData = rightMarketData[rightMarketData['auction_id'] == olderData['auction_id']]
                # If empty, that means the entire row expired, sold or was canceled
                if newerData.empty:
                    #print(f"{olderData['auction_id']} empty")
                    # If the time_left was SHORT -> when assume it expired
                    if olderData['time_left'] == 'SHORT':
                        #print(f"{olderData['auction_id']} expired")
                        singleMarketActions['amountExpired'] = singleMarketActions['amountExpired'] + olderData['quantity']
                    else:
                        # If the time_left was not SHORT and it was the newest auction -> we assume it was bought
                        if cutOffAuctionId <= olderData['auction_id']:
                            #print(f"{olderData['auction_id']} was sold")
                            singleMarketActions['amountSold'] = singleMarketActions['amountSold'] + olderData['quantity']
                        # If we don't assume it was bought and the time_left was MEDIUM -> we assume it expired
                        elif olderData['time_left'] == 'MEDIUM':
                            #print(f"{olderData['auction_id']} expired")
                            singleMarketActions['amountExpired'] = singleMarketActions['amountExpired'] + olderData['quantity']
                        # If the time_left was not SHORT or MEDIUM and it was not the newest auction -> we assume it was canceled by the seller
                        else:
                            #print(f"{olderData['auction_id']} was canceled")
                            singleMarketActions['amountCanceled'] = singleMarketActions['amountCanceled'] + olderData['quantity']

                # Else not empty, means only part of the auction sold
                else:
                    #print(f"{olderData['auction_id']} not empty")
                    quantityChange = olderData['quantity'] - newerData['quantity']
                    #print(f"Qunatity sold: {str(quantityChange)}")
                    singleMarketActions['amountSold'] = singleMarketActions['amountSold'] + int(quantityChange)
                
            # Added / Still Up (from a partially sold auction)
            for index, newerData in rightMarketData.iterrows():
                #print("-------TEST for Added--------")
                olderData = leftMarketData[leftMarketData['auction_id'] == newerData['auction_id']]
                # If empty, that means it is a newly added auction
                if olderData.empty:
                    #print(f"{newerData['auction_id']} is new")
                    singleMarketActions['amountAdded'] = singleMarketActions['amountAdded'] + newerData['quantity']

                # Else not empty, means only part of the auction sold
                # This auction is part of older auction that is still up
                else:
                    #print(f"{newerData['auction_id']} not empty")
                    #print(f"{newerData['auction_id']} part of old auction")
                    print()

            # Append item data to total market actions dataframe
            totalMarketActions = totalMarketActions.append(singleMarketActions, ignore_index=True)

            #print("-----END--------")

        # Set datetime for output file
        curDatetime = fileList[0].split('.')[0]

        # Save data to file in specific folder
        filename = datetime.now().strftime(f'ahData/{region}/{server}/marketVolume_{curDatetime}.csv')
        totalMarketActions.to_csv(filename, index=False)
        print(f'Saved data to ahData/{region}/{server}/marketVolume_{curDatetime}.csv')

#   Saved summed data to serverMarketVolume db
#       DB structure: item_id, sold, canceled, expired, datetime (to hour)
# Goal
Analysing data collected from the auction house API for Eredar-EU, World of Warcraft. Try identify best sellers, quick sellers, and items to avoid.

## WOW AH
This script will collect active auctions from the Blizzard API, on the Ereadr-EU Server then save them to a hosted database

## Preproc
This script performs some preprocessing, gathering the data in a cleaner format, identifies which items potentially sold (between 11pm 22 September and 1am 30th September) and exports to a csv. This needs a minimum of 50 hours to determine sales.
It works by gathering auctions first posted 1 hour after the initial collection, and checks which auctions where posted for at least 12 hours (minimum new duration). Essentially, any new items posted will be identified as 'sold' if they a) sell or b) are removed by seller before original duration expires. 

## Analyse
The following charts are depicted
* Daily bar chart displaying the price spread
* Hourly pricing heat map to find patterns, trends and/or correlations 
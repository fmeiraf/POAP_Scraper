# DiamondDAO scrapper example

This is an example scrapper project. This scrapper goes on the SnapShot API, and scrapes all of the available data from the GraphQL interface provided by Snapshot. 

## How to use the Docker image
First build the Docker image by running the docker-compose `docker-compose build`. Then to use the docker image you need to set ENV variables. The docker-compose sets default variables for you. The following ENV are settable:

```
#      This is the base URL for the GraphQL API of Snapshot, or a snapshot clone if one appears
BASE_URL # defaults to https://hub.snapshot.org/graphql
#      To change the default outpath for this image, you can set the OUTPATH variable. This should not be changed without changing the volumes parameters, else you will not be able to save the results anywhere.
OUTPATH # defaults to /results
#      Change this env from None to a space ID (example space.eth) to restrict the scrapper to only one space
SPACE_ID # defaults to None
``` 

## How to use the Python scrapper 
This scrapper was coded in Python, and uses ArgParse for the specification of some key parameters.

```
usage: SnapshotScrapper.py [-h] -b base_url -o out_path [-s space_id]

Snapshot DAO Scrapper.

optional arguments:
  -h, --help            show this help message and exit
  -b base_url, --url base_url
                        The base url for the Discourse forum to scrape data from, starting with http:// or https://
  -o out_path, --out out_path
                        The location for the folder containing the result of the scrapping.
  -s space_id, --space space_id
                        IF restricting to one space (DAO), the id of that DAO. For example: yam.eth
```

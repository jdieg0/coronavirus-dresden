#!/usr/bin/env python3

# Constants

JSON_URL = 'https://services.arcgis.com/ORpvigFPJUhb8RDF/arcgis/rest/services/corona_DD_7_Sicht/FeatureServer/0/query?f=json&where=ObjectId>=0&outFields=*'

# Debugging
from IPython import embed

# JSON
import urllib.request
import json

def main():
    with urllib.request.urlopen(JSON_URL) as response:
        data = json.load(response)
        print (data)

if __name__ == '__main__':
    main()
# Coronavirus Dresden
This script collects official infection statistics published by the city of Dresden and saves them to [InfluxDB](https://www.influxdata.com/products/influxdb-overview/). From there the data can be processed and visualised using SQL and, for instance, [Grafana](https://grafana.com/docs/grafana/latest/datasources/influxdb/).

Subsequent changes to the published data set can also be detected and routinely logged.

## Data source

The raw data provided by the [city of Dresden](https://www.dresden.de/de/leben/gesundheit/hygiene/infektionsschutz/corona.php) and visualised on their [Dashboard](https://stva-dd.maps.arcgis.com/apps/opsdashboard/index.html#/3eef863531024aa4ad0c4ac94adc58e0) can be obtained from the following source:

- [Overview over all tables and data fields](https://services.arcgis.com/ORpvigFPJUhb8RDF/ArcGIS/rest/services/corona_DD_7_Sicht/FeatureServer/layers)
- [Web form for queries](https://services.arcgis.com/ORpvigFPJUhb8RDF/ArcGIS/rest/services/corona_DD_7_Sicht/FeatureServer/query)
- [Download the latest JSON dump](https://services.arcgis.com/ORpvigFPJUhb8RDF/arcgis/rest/services/corona_DD_7_Sicht/FeatureServer/0/query?f=json&where=ObjectId>=0&outFields=*)

## Install

Using a virtual environment of your choice is recommended. An exemplary installation with ```venv``` is described below.

### venv (recommended)

	python3 -m venv venv
	source venv/bin/activate

    pip install -r requirements.txt

### InfluxDB (required)

#### [macOS](https://docs.influxdata.com/influxdb/v1.8/introduction/install/)

	brew install influxdb
	brew services start influxdb

Helpful resources:

- [Getting Started with Python and InfluxDB](https://www.influxdata.com/blog/getting-started-python-influxdb/)

### Grafana (optional)

#### [mac OS](https://grafana.com/docs/grafana/latest/installation/mac/)

    brew install grafana
    brew services start grafana

## Run

    python collect.py

### Command line arguments

To display all data collection options, type:

    python collect.py --help
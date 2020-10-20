# Coronavirus Dresden
This script collects official infection statistics published by the city of Dresden and saves them to [InfluxDB](https://www.influxdata.com/products/influxdb-overview/). From there the data can be processed and visualised using SQL and, for instance, [Grafana](https://grafana.com/docs/grafana/latest/datasources/influxdb/).

Subsequent changes to the published data set can also be detected and routinely logged.

## Install

Using a virtual environment of your choice is recommended. An exemplary installation with ```venv``` is described below.

### venv

	python3 -m venv venv
	source venv/bin/activate

    pip install -r requirements.txt

## Run

    python collect.py

### Command line arguments

To display all data collection options, type:

    python collect.py --help
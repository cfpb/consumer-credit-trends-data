# consumer-credit-trends-data

![Screenshot of consumer-credit-trends](image.png)

## Explore recent developments in consumer credit markets
This data appears on http://www.consumerfinance.gov/data-research/consumer-credit-trends/ powering graphs for each featured credit market.

## We want your feedback, but will not be able to respond to everyone
We want as much feedback as possible to help us make informed decisions so that we can make this tool better. Unfortunately, we will not be able to respond to every piece of feedback or comment we receive, but intend to respond with our progress through the evolution of the tool.

## Contents

1. Folders per released market containing CSV and JSON files, using a schema described below.
  1. JSON files are rendered on our live site using https://github.com/cfpb/cfpb-chart-builder, an extension of Highcharts.
1. `process_incoming_data.py` transforms internal Office of Research summary CSVs into a more user-readable format.

## Data schema

Markets:

- auto-loans (AUT)
- credit-cards (CRC)
- mortgages (MTG)
- student-loans (STU)

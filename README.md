# When I Work Code Challange
A processing pipeline to extract data from a url endpoint transform and pivot it using certain fields in the initial data.

## Summary
It comprises of a DataProcess class with claass functions that takes in a url as input downloads the web trafficdata in csv from the url, transforms the web traffic data stored in time-record format where
each row is a page view into a per-user format where each row is adifferent user and the
columns represent the time spent on each of the pages.
: user_id (index column)
: path (pivoted column)
: length (values under each column or user_id row after pivoting)

Parameters:
- url (str): The URL to be validated.
- column: column used to pivot
- index: index used as rows on the new pivoted table
- value: value of each records
- destination_file_name: the destination of the generated csv file,

Returns:
- output: A saved csv file with the process data according to therequirement


### Set Up for Dependencies
1. Install Requirement:  run the below on the command line
```bash
pip install -r requirements.txt
```

### Run the pipeline
```bash

   python3 pipeline.py --url "your_endpoint"   --column "your_column"  --index "your_index_column" --value "your_value_column" --destination_file_name "output_file"  --destination_file_path  "destinationof_the_file"

```

### URL Endpoint

```bash
 --url "https://public.wiwdata.com/engineering-challenge/data/"  
```

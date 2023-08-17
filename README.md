# Clickstream Data Pipeline

_Contains code and other configurations required to process clickstream data

## Use Case
The pipeline reads static data, transform, performs data quality checks and load data to a structured database and draw insights out of the dataset in form of visualizations.

## Pipeline Flow-chart
![](../../../../Users/kkweb/Downloads/architecture.drawio.png)

### Source
Source of data for this pipeline is the clickstream and item dataset in the CSV format.

### Destination
The transformed data is finally written to a mySQL table using spark JDBC. Below is the production table:

```
target_ready_prod.clickstream
```

### About the Code

The code reads data from input source directory and write it into kafka stream. After loading data from kafka topic into a dataframme it performs various cleansing operations (case conversion, elimination NULLs and duplicates, concatenate, split, trim, lowercase and uppercase, dataframe columns) and transformation (joins). It loads the transformed data to a stage table in mySQL, on top of which data quality checks are performed. When the checks pas, the data is finally written to production table for users to consume.

### Instructions

To run the code in your environment change the following variables in ApplicationConstants and mySqlConfig file

````
SERVER_ID
TOPIC_NAME_ITEM_DATA
TOPIC_NAME_CLICKSTREAM_DATA
DB_NAME
MYSQL_SERVER_ID_PORT
USER_NAME
KEY_PASSWORD
STAGING_TABLE
PROD_TABLE
Username: mySqlConfig.properties 
Password: mySqlConfig.properties 
Priver: mySqlConfig.properties 
````



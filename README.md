# Technical Test
Public repo of code for the Colibri technical test.

## Instructions
Consider the following scenario:
You are a data engineer for a renewable energy company that operates a farm of wind turbines.
The turbines generate power based on wind speed and direction, and their output is measured in megawatts (MW). 

Your task is to build a data processing pipeline that ingests raw data from the turbines and performs the following operations:
● Cleans the data: The raw data contains missing values and outliers, which must be removed or imputed.
● Calculates summary statistics: For each turbine, calculate the minimum, maximum, and average power output over a given time period (e.g., 24 hours).
● Identifies anomalies: Identify any turbines that have significantly deviated from their expected power output over the same time period. Anomalies can be defined as turbines whose output is outside of 2 standard deviations from the mean.
● Stores the processed data: Store the cleaned data and summary statistics in a database for further analysis.

Data is provided to you as CSVs which are appended daily. Due to the way the turbine measurements are set up, each csv contains data for a group of 5 turbines. Data for a particular turbine will always be in the same file (e.g. turbine 1 will always be in data_group_1.csv). Each day the csv will be updated with data from the last 24 hours, however the system is known to sometimes miss entries due to sensor malfunctions.

The files provided in the attachment represent a valid set for a month of data recorded from the 15 turbines. Feel free to add/remove data from the set provided in order to test/satisfy the requirements above.
Your pipeline should be scalable and testable; emphasis is based on the clarity and quality of the code and the implementation of the functionality outlined above, and not on the overall design of the application.

Your solution should be implemented in Python, using any frameworks or libraries that you deem appropriate. Please provide a brief description of your solution design and any assumptions made in your implementation.

## Solution

### Tech
The solution will use Databricks as the primary platform for ingesting, transforming, and storing the data in the final database.  
Code is kept in Notebooks and written in PySpark.  The final database can be accessed using a SQL Warehouse and the SQL language, which analysts are commonly more familiar with.

The common Lakehouse best practice of Bronze-Silver-Gold (medallion architecture) will store and transform the data as follows:
- Bronze = raw data
- Silver = cleansed and conformed data: Delta tables storing data cleansed, validates, and with missing values imputed
- Gold = presentation data: stored in Delta tables for further querying, with an abstraction layer of Views for end-clients to read from

**Bronze**
Daily CSV files are dropped in a Blob store.  File writes trigger AutoLoader to read the data and convert to Bronze Delta tables with the same schema, but with metadata columns added to aid with potential future tasks like dealing with late-arriving data.

**Silver**
Reads the data in Bronze and:
- conforms table and column names
- de-normalizes key columns
- validates row completeness, and imputes missing values where necessary
- validates data ranges


## Out of Scope
This solution has been designed and built on a 'proof of concept' basis.  Out of scope features (that would be part of a final design) include:
- environments (dev, test, or prod)
- CI/CD
- Infrastructure as Code

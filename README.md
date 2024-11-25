# Technical Test
Public repo of code for the Colibri technical test.

## Instructions
<i>
Consider the following scenario:
You are a data engineer for a renewable energy company that operates a farm of wind turbines.
The turbines generate power based on wind speed and direction, and their output is measured in megawatts (MW). 

Your task is to build a data processing pipeline that ingests raw data from the turbines and performs the following operations:
- Cleans the data: The raw data contains missing values and outliers, which must be removed or imputed.
- Calculates summary statistics: For each turbine, calculate the minimum, maximum, and average power output over a given time period (e.g., 24 hours).
- Identifies anomalies: Identify any turbines that have significantly deviated from their expected power output over the same time period. Anomalies can be defined as turbines whose output is outside of 2 standard deviations from the mean.
- Stores the processed data: Store the cleaned data and summary statistics in a database for further analysis.

Data is provided to you as CSVs which are appended daily. Due to the way the turbine measurements are set up, each csv contains data for a group of 5 turbines. Data for a particular turbine will always be in the same file (e.g. turbine 1 will always be in data_group_1.csv). Each day the csv will be updated with data from the last 24 hours, however the system is known to sometimes miss entries due to sensor malfunctions.

The files provided in the attachment represent a valid set for a month of data recorded from the 15 turbines. Feel free to add/remove data from the set provided in order to test/satisfy the requirements above.
Your pipeline should be scalable and testable; emphasis is based on the clarity and quality of the code and the implementation of the functionality outlined above, and not on the overall design of the application.

Your solution should be implemented in Python, using any frameworks or libraries that you deem appropriate. Please provide a brief description of your solution design and any assumptions made in your implementation.
</i>

<hr />

## Solution

### Tech
The solution uses Databricks as the primary platform for ingesting, transforming, and storing the data in the final database.  
Pipeline code is in Notebooks, written in PySpark, and checked-in to this GitHub repo.  DDL code is in SQL, and also checked into this repo.  The Gold tier is accessible from a SQL Warehouse using the SQL language, which analysts are commonly more familiar with.

The common Lakehouse best practice of Bronze-Silver-Gold (medallion architecture) will store and transform the data as follows:
- Bronze = raw data
- Silver = cleansed and conformed data
- Gold = presentation data

**Bronze**
Daily CSV files are dropped in a Blob store.  A Workflow reads the data and converts it to Bronze Delta tables with the same schema, but with audit columns added to aid with potential future tasks like dealing with late-arriving data.

**Silver**
Reads the data in Bronze and:
- conforms table and column names
- de-normalizes the timestamp column
- validates row completeness, and imputes missing values where necessary
- validates data ranges

**Gold**
 The 'presentable' version of the data.  The table includes columns indicating which rows had values imputed (for audit and data quality purposes), but these columns are not exposed in the View on top of the Gold table.
 Dashboards read from the Views and provide an easily-accessible summary of the data including:
 - min
 - max
 - avg
 of each turbine per hour per day.

### Workflow
Databricks Workflows call the Bronze - Silver - Gold Notebooks in sequence.  It can also be set up on a schedule (e.g. daily) to process new files dropped in the Storage Container (NB: although adding a Schedule is out of scope for this exercise.  If the files will be uploaded more regularly than daily then a more elegant solution would be to use AutoLoader and stream the data from the file into the Lakehouse.)

Screenshots of an example Workflow:
![20241125_workflow_status](https://github.com/user-attachments/assets/906af2b5-94f8-44ad-988e-a1d7297fdcd1)

![20241125_workflow_run](https://github.com/user-attachments/assets/bbb9f683-cbcc-4b7b-9774-7aa66306b1af)


## Out of Scope
This solution has been designed and built on a 'proof of concept' basis.  Out of scope features (that would be part of a final design) include:
- environments (dev, test, or prod)
- CI/CD
- Infrastructure as Code

CI/CD for this solution can be accomplished using Databricks Asset Bundles:  
https://docs.databricks.com/en/dev-tools/bundles/index.html

Also out of scope because of limitations on MSDN Azure Subscriptions is integration with Unity Catalog.  And some of the Premium features are only available for a 14-day period.

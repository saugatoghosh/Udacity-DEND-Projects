## Project: Data Pipelines With Airflow

A music streaming company, Sparkify, has decided that it is time to introduce more automation and monitoring 
to their data warehouse ETL pipelines and come to the conclusion that the best tool to achieve this is Apache Airflow.

In the project we are expected to create high grade data pipelines that are dynamic and built from reusable tasks, 
can be monitored, and allow easy backfills. Sparkify has also noted that the data quality plays a big part when analyses
are executed on top the data warehouse and want to run tests against their datasets after the ETL steps have been executed 
to catch any discrepancies in the datasets.

The source data resides in S3 and needs to be processed in a data warehouse in Amazon Redshift. 
The source datasets consist of JSON logs that tell about user activity in the application and JSON metadata about the 
songs the users listen to.

To complete the project, we have create our own custom operators to perform tasks such as staging the data, filling the 
data warehouse, and running checks on the data as the final step.


The following steps have been used to extract the source data, transform them into fact and dimension tables and then 
load them into the data warehouse in Amazon Redshift:

- Create the fact, dimension and staging tables in the Amazon redshift cluster directly
- Extract the data from the source datasets into staging tables using a custom stage operator
- Transform the data into fact and dimensions tables and load into redshift using custom fact and dimension operators
- Perform data quality checks on the loaded dimension tables using a data quality operator

Screenshots of example sql queries performed on the tables in the redshift cluster are added to the queries folder.


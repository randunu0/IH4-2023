# Database Documentation

The purpose of this document is to inform you on how the data for the web app is collected and managed.

## MySQL Database

The data for the web app is stored in a MySQL database instance hosted by Amazon Relational Database Service (RDS). At the moment, the database holds the following tables:

* GBFT: Generation by Fuel Type
* RTSC: Real-Time System Conditions
* SEL: State Estimator Load
* SMPP_LZ: Settlement Point Prices by Load Zone
* SPP: Solar Power Production
* SWD: System-Wide Demand
* WPP: Wind Power Production

### Database Configuration

The database is hosted by RDS and takes advantage of the free tier provisions. It utilizes the MySQL database management system and is a db.t2.micro class. The database has the following specifications:

* 50 GiB Storage
* 1 vCPU
* 1 GB RAM

Further information about the RDS Free tier can be found on this website: <https://aws.amazon.com/rds/free/>


Further information about the db.t2.micro class specifications can be found on this website under the T2 tab: <https://aws.amazon.com/rds/instance-types/>

### Connecting to the Database

Connection to the database is currently private to the contributors. If you have any questions regarding how to access the database, please contact either Ryan Krogfoss or Umar Burney. Their GitHub accounts can be found on the git page.

## Updating the Database

The following subsections detail the methods used to update the database tables. All tables source data which is publically available on the ERCOT website <https://www.ercot.com/gridinfo>. Most of the tables are updated using an AWS Lambda script that is called periodically by Amazon EventBridge triggers.

### MIS Tables

The following tables utilize AWS Lambda to fetch data from the ERCOT website and add it to the database: 

* SEL: State Estimator Load
* SMPP_LZ: Settlement Point Prices by Load Zone
* SPP: Solar Power Production
* SWD: System-Wide Demand
* WPP: Wind Power Production

The data for these tables are found under the misapp subdomain of the ERCOT website and stored as .csv and .xml files. We utilize AWS Lambda to periodically scrape the respective websites for recent data, parse the data into the format that is compatible with our database, and append the new data into their respective tables.

The update frequencies are as follows:

1 Hour:
* SEL: State Estimator Load
* SWD: System-Wide Demand

15 Minutes:
* SMPP_LZ: Settlement Point Prices by Load Zone

5 Minutes: 
* SPP: Solar Power Production
* WPP: Wind Power Production

### Real Time System Conditions

Unlike the MIS Tables, the data for Real-Time System Conditions is not stored in a .csv or .xml file. Instead, the data is displayed on an HTML table that updates every 10 seconds. 

We created an AWS Lambda script to periodically scrape the table every minute, parse the data, and append it to the RTSC table of our database. 

The update frequency is set at 1 minute due to the limitations of the AWS Event Bridge service. 

### Generation by Fuel Type

The data for Generation by Fuel Type is accessible from this page of the ERCOT website <https://www.ercot.com/gridinfo/generation> under the Fuel Mix section.

The information is stored as a .xlsx file which has caused issues due to incompatibilities with how we currently use AWS Lambda to parse the information. As a result, the GBFT table is currently updated manually monthly using the updateGBFT.py script under the scripts directory. 

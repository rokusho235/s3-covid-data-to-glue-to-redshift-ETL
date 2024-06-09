# s3-covidData-to-glue-to-redshift-ETL

ETL Pipeline: S3 > Glue Crawlers > Athena > Glue Python Job > S3 > Redshift

### Architecture Diagram

![Architecture Diagram](https://raw.githubusercontent.com/rokusho235/s3-covidData-to-glue-to-redshift-ETL/main/covidProjectArch.png)

### Services Used

1.  **Amazon S3**
2.  **AWS Glue**
3.  **Amazon Athena**
4.  **Amazon Redshift**

### OLTP Table Data Modeling

![Data Model](https://raw.githubusercontent.com/rokusho235/s3-covidData-to-glue-to-redshift-ETL/main/covidDataModel.png)

### OLAP Table Dimensional Modeling

![Star Schema](https://raw.githubusercontent.com/rokusho235/s3-covidData-to-glue-to-redshift-ETL/main/covidStarSchema.png)

CREATE EXTERNAL TABLE complaints_parquet (
    `Date_received` DATE,
    `Product` STRING,    
    `Sub_product` STRING,
    `Issue` STRING,      
    `Sub_issue` STRING,
    `Consumer_complaint_narrative` STRING,
    `Company_public_response` STRING,     
    `Company` STRING,
    `State` STRING,  
    `ZIP_code` STRING,
    `Tags` STRING,    
    `Consumer_consent_provided` STRING,
    `Submitted_via` STRING,
    `Date_sent_to_company` STRING,
    `Company_response_to_consumer` STRING,
    `Timely_response` STRING,
    `Consumer_disputed` STRING,
    `Complaint_ID` STRING
)
STORED AS PARQUET
LOCATION '/user/hive/warehouse/complaints_parquet/'
TBLPROPERTIES ("parquet.compression"="SNAPPY");
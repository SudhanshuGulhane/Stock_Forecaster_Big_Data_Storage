hdfs dfs -mkdir -p /user/hive/warehouse/amex_findata
hdfs dfs -put -f /tmp/amex_findata.csv /user/hive/warehouse/amex_findata/

hdfs dfs -put /tmp/capone_findata.csv /user/hive/warehouse/capone_findata/

amex_findata
boa_findata
capone_findata
citibank_findata
discover_findata
jpmorgan_findata
navient_findata
synchrony_findata
usbancorp_findata
wellsfargo_findata

CREATE EXTERNAL TABLE wellsfargo_findata (
    record_date DATE,
    open_price FLOAT,
    high_price FLOAT,
    low_price FLOAT,
    close_price FLOAT
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe'
WITH SERDEPROPERTIES (
    "serialization.format" = ",",
    "field.delim" = ","
)
STORED AS TEXTFILE
LOCATION '/user/hive/warehouse/wellsfargo_findata/'
TBLPROPERTIES ("skip.header.line.count"="1");

CREATE EXTERNAL TABLE complaints_table (
    `Date received` DATE,
    `Product` STRING,
    `Sub-product` STRING,
    `Issue` STRING,
    `Sub-issue` STRING,
    `Consumer complaint narrative` STRING,
    `Company public response` STRING,
    `Company` STRING,
    `State` STRING,
    `ZIP code` STRING,
    `Tags` STRING,
    `Consumer consent provided?` STRING,
    `Submitted via` STRING,
    `Date sent to company` STRING,
    `Company response to consumer` STRING,
    `Timely response?` STRING,
    `Consumer disputed?` STRING,
    `Complaint ID` STRING
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe'
WITH SERDEPROPERTIES (
    "serialization.format" = ";",
    "field.delim" = ";"
)
STORED AS TEXTFILE
LOCATION '/user/hive/warehouse/complaints_table/'
TBLPROPERTIES ("skip.header.line.count"="1");




SELECT
    c.`date received` AS `date`,
    c.company,
    COUNT(c.`complaint id`) AS complaint_count,
    f.close_price AS close_price,
    f.close_price - LAG(f.close_price) OVER (
        PARTITION BY c.company 
        ORDER BY c.`date received`
    ) AS close_price_difference
    FROM
        complaints_table c
    JOIN
        amex_findata f
    ON
        f.record_date = DATE_SUB(c.`date received`, 1)
    WHERE
        c.company = 'AMERICAN EXPRESS COMPANY'
        AND c.`date received` BETWEEN '2023-01-01' AND '2023-12-31'
    GROUP BY
        c.`date received`, c.company, f.close_price
    ORDER BY
        c.`date received`;


WITH complaints_summary AS (
    SELECT
        c.`date received` AS `date`,
        c.company,
        COUNT(c.`complaint id`) AS complaint_count
    FROM complaints_table c
    WHERE c.company = 'AMERICAN EXPRESS COMPANY'
    AND c.`date received` BETWEEN '2023-01-01' AND '2023-12-31'
    GROUP BY c.`date received`, c.company
    ),
    financial_data AS (
        SELECT
            f.record_date,
            f.close_price,
            f.close_price - LAG(f.close_price) OVER (
                PARTITION BY f.record_date 
                ORDER BY f.record_date
            ) AS close_price_difference
        FROM amex_findata f
    )
    SELECT
        cs.`date`,
        cs.company,
        cs.complaint_count,
        fd.close_price,
        fd.close_price_difference
    FROM complaints_summary cs
    JOIN financial_data fd
    ON fd.record_date = DATE_SUB(cs.`date`, 1)
    ORDER BY cs.`date`;



CREATE EXTERNAL TABLE complaints_parquet (
    `Date received` DATE,
    `Product` STRING,
    `Sub-product` STRING,
    `Issue` STRING,
    `Sub-issue` STRING,
    `Consumer complaint narrative` STRING,
    `Company public response` STRING,
    `Company` STRING,
    `State` STRING,
    `ZIP code` STRING,
    `Tags` STRING,
    `Consumer consent provided?` STRING,
    `Submitted via` STRING,
    `Date sent to company` STRING,
    `Company response to consumer` STRING,
    `Timely response?` STRING,
    `Consumer disputed?` STRING,
    `Complaint ID` STRING
)
STORED AS PARQUET
LOCATION '/user/hive/warehouse/complaints_parquet/'
TBLPROPERTIES ("parquet.compression"="SNAPPY", "skip.header.line.count"="1");





SELECT
    c.`date received` AS `date`,
    c.company,
    COUNT(c.`complaint id`) AS complaint_count,
    f.close_price AS close_price,
    f.close_price - LAG(f.close_price) OVER (
        PARTITION BY c.company 
        ORDER BY c.`date received`
    ) AS close_price_difference
    FROM
        complaints_table c
    JOIN
        synchrony_findata f
    ON
        f.record_date = DATE_SUB(c.`date received`, 1)
    WHERE
        c.company = 'SYNCHRONY FINANCIAL'
        AND c.`date received` BETWEEN '2023-01-01' AND '2023-12-31'
    GROUP BY
        c.`date received`, c.company, f.close_price
    ORDER BY
        c.`date received`;



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



SELECT
    c.`date_received` AS `date`,
    c.company,
    COUNT(c.`complaint_id`) AS complaint_count,
    f.close_price AS close_price,
    f.close_price - LAG(f.close_price) OVER (
        PARTITION BY c.company 
        ORDER BY c.`date_received`
    ) AS close_price_difference
    FROM
        complaints_parquet c
    JOIN
        synchrony_findata f
    ON
        f.record_date = DATE_SUB(c.`date_received`, 1)
    WHERE
        c.company = 'SYNCHRONY FINANCIAL'
        AND c.`date_received` BETWEEN '2023-01-01' AND '2023-12-31'
    GROUP BY
        c.`date_received`, c.company, f.close_price
    ORDER BY
        c.`date_received`;
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
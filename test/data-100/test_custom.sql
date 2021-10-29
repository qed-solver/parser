CREATE TABLE indiv_sample_nyc(    
	cmte_id INT,
    transaction_amt INT,
    name VARCHAR(10)
    );

SELECT
    cmte_id,
    transaction_amt,
    name
FROM indiv_sample_nyc
WHERE  (name LIKE '%TRUMP%') AND  (name LIKE '%DONALD%') 
GROUP BY cmte_id, transaction_amt, name








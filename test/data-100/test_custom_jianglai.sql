CREATE TABLE indiv_sample_nyc(    
	cmte_id INT,
    transaction_amt INT,
    name VARCHAR(10)
    );

SELECT cmte_id, SUM(transaction_amt)
FROM indiv_sample_nyc
GROUP BY cmte_id;

SELECT cmte_id, SUM(transaction_amt)
FROM indiv_sample_nyc
GROUP BY cmte_id
HAVING SUM(transaction_amt) > 10 AND MIN(transaction_amt) > 2;

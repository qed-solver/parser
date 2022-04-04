CREATE TABLE indiv_sample_nyc(    
	cmte_id INT,
    transaction_amt INT,
    name VARCHAR(10),
    str_name VARCHAR(10)
    );

SELECT * FROM indiv_sample_nyc WHERE cmte_id = 1 AND name LIKE '%test%' AND str_name LIKE '%test3%';

SELECT * FROM indiv_sample_nyc WHERE cmte_id = 2 AND name LIKE '%test%' AND str_name LIKE '%test3%';
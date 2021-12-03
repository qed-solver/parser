CREATE TABLE indiv_sample_nyc(    
	cmte_id INT,
    transaction_amt INT,
    name VARCHAR(10)
    );

CREATE TABLE indiv_sample_sf(    
	cmte_id INT,
    transaction_amt INT,
    name VARCHAR(10)
    );

SELECT * FROM indiv_sample_nyc LEFT JOIN indiv_sample_sf ON indiv_sample_nyc.cmte_id = indiv_sample_sf.cmte_id;

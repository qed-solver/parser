CREATE TABLE indiv_sample_nyc(    
	cmte_id INT,
    transaction_amt INT,
    name VARCHAR(10)
    );

CREATE TABLE indiv_sample_sf(    
	id INT,
    tx_amt INT,
    description VARCHAR(10)
    );

SELECT * FROM indiv_sample_nyc RIGHT JOIN indiv_sample_sf ON indiv_sample_nyc.cmte_id = indiv_sample_sf.id;

SELECT cmte_id, name FROM indiv_sample_nyc;

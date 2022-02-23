CREATE TABLE nyc(    
	cmte_id INT,
    transaction_amt INT,
    name VARCHAR(10)
    );

CREATE TABLE sf(    
	cmte_id INT,
    tx_amt INT,
    description VARCHAR(10)
    );

SELECT * FROM nyc LEFT JOIN sf ON nyc.cmte_id = sf.cmte_id;

SELECT cmte_id, name FROM nyc;

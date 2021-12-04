CREATE TABLE nyc(    
	cmte_id INT,
    transaction_amt INT,
    name VARCHAR(10)
    );

CREATE TABLE sf(    
	id INT,
    tx_amt INT,
    description VARCHAR(10)
    );

SELECT * FROM nyc JOIN sf ON nyc.cmte_id = sf.id WHERE nyc.cmte_id = 1;

SELECT cmte_id, name FROM nyc;

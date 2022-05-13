CREATE TABLE Customers(
 id INT,
 name VARCHAR(10)
 );

CREATE TABLE Orders(
 id INT,
 customerId INT,
 price INT
 );

SELECT * FROM Customers WHERE id = 3;

SELECT * FROM Customers WHERE name = '%Smith';
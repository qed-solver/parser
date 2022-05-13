#lang rosette

(require "../util.rkt" "../sql.rkt" "../table.rkt"  "../evaluator.rkt" "../equal.rkt" "../cosette.rkt" "../denotation.rkt" "../syntax.rkt")

(define CUSTOMERS (Table "CUSTOMERS" (list "ID" "NAME") (gen-sym-schema 2 1)))
(define ORDERS (Table "ORDERS" (list "ID0" "CUSTOMERID" "PRICE") (gen-sym-schema 3 1)))


(define q1s (SELECT (VALS "CUSTOMERS.ID" "CUSTOMERS.NAME") FROM (NAMED CUSTOMERS) WHERE (BINOP "CUSTOMERS.ID" = 3)))

(define q2s (SELECT (VALS "CUSTOMERS.ID" "CUSTOMERS.NAME") FROM (NAMED CUSTOMERS) WHERE (BINOP "CUSTOMERS.NAME" = '%Smith')))


(cond
	[(eq? #f #t) println("LIKE regex does not match")]
	[(eq? #f #t) println("ORDER BY does not match")]
	[(eq? #t #t)
		(let* ([model (verify (same q1s q2s))]
			   [concrete-t1 (clean-ret-table (evaluate CUSTOMERS model))]
			   [concrete-t2 (clean-ret-table (evaluate ORDERS model))])
			(println concrete-t1)
			(println concrete-t2)
)])
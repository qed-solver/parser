#lang rosette

(require "../util.rkt" "../sql.rkt" "../table.rkt"  "../evaluator.rkt" "../equal.rkt" "../cosette.rkt" "../denotation.rkt" "../syntax.rkt")

(define INDIV_SAMPLE_NYC (Table "INDIV_SAMPLE_NYC" (list "CMTE_ID" "TRANSACTION_AMT" "NAME") (gen-sym-schema 3 1)))


(define q1s (SELECT (VALS "INDIV_SAMPLE_NYC.CMTE_ID" aggr-sum  "INDIV_SAMPLE_NYC.TRANSACTION_AMT") FROM (NAMED INDIV_SAMPLE_NYC) WHERE (TRUE) GROUP-BY (list "INDIV_SAMPLE_NYC.CMTE_ID")))

(define q2s (SELECT (VALS "INDIV_SAMPLE_NYC.CMTE_ID") FROM (NAMED INDIV_SAMPLE_NYC) WHERE (TRUE) GROUP-BY (list "INDIV_SAMPLE_NYC.CMTE_ID") HAVING (BINOP (VAL-UNOP aggr-sum "INDIV_SAMPLE_NYC.TRANSACTION_AMT") > 10)))


(cond
	[(eq? #f #t) println("LIKE regex does not match")]
	[(eq? #f #t) println("ORDER BY does not match")]
	[(eq? #t #t)
		(let* ([model (verify (same q1s q2s))]
			   [concrete-t1 (clean-ret-table (evaluate INDIV_SAMPLE_NYC model))])
			(println concrete-t1)
)])
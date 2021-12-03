#lang rosette

(require "../util.rkt" "../sql.rkt" "../table.rkt"  "../evaluator.rkt" "../equal.rkt" "../cosette.rkt")

(define INDIV_SAMPLE_NYC (Table "INDIV_SAMPLE_NYC" (list "CMTE_ID" "TRANSACTION_AMT" "NAME") (gen-sym-schema 3 1)))
(define INDIV_SAMPLE_SF (Table "INDIV_SAMPLE_SF" (list "CMTE_ID" "TRANSACTION_AMT" "NAME") (gen-sym-schema 3 1)))


(define q1s (SELECT (VALS "INDIV_SAMPLE_NYC.CMTE_ID" "INDIV_SAMPLE_NYC.TRANSACTION_AMT" "INDIV_SAMPLE_NYC.NAME"(define q1s ())(define q1s ())(define q1s ())) WHERE (TRUE)))


(let* ([model (verify (same q1s q2s))]
	   [concrete-t1 (clean-ret-table (evaluate INDIV_SAMPLE_NYC model))]
	   [concrete-t2 (clean-ret-table (evaluate INDIV_SAMPLE_SF model))])
	(println concrete-t1)
	(println concrete-t2)
)
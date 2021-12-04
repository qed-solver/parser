#lang rosette

(require "../util.rkt" "../sql.rkt" "../table.rkt"  "../evaluator.rkt" "../equal.rkt" "../cosette.rkt")

(define SF (Table "SF" (list "ID" "TX_AMT" "DESCRIPTION") (gen-sym-schema 3 1)))

(define q1s (SELECT (VALS "NYC_JOIN_SF.CMTE_ID" "NYC_JOIN_SF.TRANSACTION_AMT" "NYC_JOIN_SF.NAME" "NYC_JOIN_SF.ID" "NYC_JOIN_SF.TX_AMT" "NYC_JOIN_SF.DESCRIPTION") FROM (AS (JOIN (NAMED NYC) (NAMED SF)) [ "NYC_JOIN_SF" (list "CMTE_ID" "TRANSACTION_AMT" "NAME" "ID" "TX_AMT" "DESCRIPTION") ]) WHERE (AND (BINOP "NYC_JOIN_SF.CMTE_ID" = "NYC_JOIN_SF.ID") (BINOP "NYC_JOIN_SF.CMTE_ID" = 1)) ))

(define q2s (SELECT (VALS "NYC.CMTE_ID" "NYC.NAME") FROM (NAMED NYC) WHERE (TRUE)))


(let* ([model (verify (same q1s q2s))]
	   [concrete-t1 (clean-ret-table (evaluate SF model))])
	(println concrete-t1)
)
#lang rosette

(require "../util.rkt" "../sql.rkt" "../table.rkt"  "../evaluator.rkt" "../equal.rkt" "../cosette.rkt" "../denotation.rkt" "../syntax.rkt")

(define INDIV_SAMPLE_NYC (Table "INDIV_SAMPLE_NYC" (list "CMTE_ID" "TRANSACTION_AMT" "NAME") (gen-sym-schema 3 1)))

(define (gen-a)
	(define-symbolic* a boolean?)
  a)
(define (gen-b)
	(define-symbolic* b boolean?)
  b)
(define (gen-c)
	(define-symbolic* c boolean?)
  c)

(define q1s (SELECT (VALS "INDIV_SAMPLE_NYC.CMTE_ID" "INDIV_SAMPLE_NYC.TRANSACTION_AMT" "INDIV_SAMPLE_NYC.NAME") FROM (NAMED INDIV_SAMPLE_NYC) WHERE (AND (filter-sym (gen-a)) (filter-sym (gen-a)) )))

(define q2s (SELECT (VALS "INDIV_SAMPLE_NYC.CMTE_ID" "INDIV_SAMPLE_NYC.NAME") FROM (NAMED INDIV_SAMPLE_NYC) WHERE (filter-sym (gen-b))))


(cond
	[(eq? #f #f) println("LIKE regex does not match")]
	[(eq? #f #f) println("ORDER BY does not match")]
	[(eq? #t #f)
		(let* ([model (verify (same q1s q2s))]
			   [concrete-t1 (clean-ret-table (evaluate INDIV_SAMPLE_NYC model))])
			(println concrete-t1)
)])
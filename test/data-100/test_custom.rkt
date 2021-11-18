#lang rosette

(require "../util.rkt" "../sql.rkt" "../table.rkt"  "../evaluator.rkt" "../equal.rkt" "../cosette.rkt")



(define q1s (SELECT (VALS "INDIV_SAMPLE_NYC.CMTE_ID" "INDIV_SAMPLE_NYC.NAME") FROM (NAMED INDIV_SAMPLE_NYC) WHERE (TRUE)))


(let* ([model (verify (same q1s q2s))]
)
)
#lang cosette

(define r0 (r-project (list (v-op 'fund (list (v-var 1)))) (r-join 'inner (v-op '= (list (v-op '- (list (v-var 0) (v-var 2))) (v-op 0 (list)))) (r-scan 0) (r-agg (list) (list 0) (r-scan 1)))))
(define r1 (r-project (list (v-op 'fund (list (v-var 1)))) (r-filter (v-hop 'in (list (v-var 0)) (r-scan 1)) (r-scan 0))))

(define tables (list (table-info "dept" (list "deptno" "dname")) (table-info "emp" (list "deptno"))))

(solve-relations r0 r1 tables println)

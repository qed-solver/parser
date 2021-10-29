SELECT
    cmte_id,
    SUM(transaction_amt) as total_amount,
    COUNT(*) as num_donations,
    cmte_nm
FROM
        (SELECT *
        FROM indiv_sample_nyc, comm
        WHERE indiv_sample_nyc.cmte_id = comm.cmte_id)
WHERE  (name LIKE'%TRUMP%')  AND  (name LIKE '%DONALD%') AND  (name NOT LIKE '%INC%') GROUP BY cmte_id
ORDER BY total_amount DESC









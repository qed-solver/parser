SELECT
    cmte_id,
    SUM(transaction_amt) as total_amount,
    COUNT(*) as num_donations
FROM indiv_sample_nyc
WHERE  (name LIKE '%TRUMP%') AND  (name LIKE '%DONALD%') AND  (name NOT LIKE '%INC%') GROUP BY cmte_id
ORDER BY total_amount DESC








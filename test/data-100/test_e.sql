SELECT
    cmte_id AS committee_id,
    sum(transaction_amt) AS total_amount,
    count(*) AS count
FROM indiv_sample_nyc
WHERE  (cmte_id LIKE '%5') GROUP BY cmte_id
ORDER BY count DESC
LIMIT 5























































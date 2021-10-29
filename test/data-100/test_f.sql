SELECT c1.cand_name, c2.cmte_nm
FROM cand c1 INNER JOIN comm c2 ON c1.cand_id = c2.cand_id
ORDER BY c1.cand_name DESC
LIMIT 5

































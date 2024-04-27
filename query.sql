SELECT blockNumber,
       SUM(value) AS total
FROM transactions
WHERE blockNumber IN
        (SELECT number
         FROM blocks
         WHERE timestamp >= '2024-01-01T00:00:00'
             AND timestamp <= '2024-01-01T00:30:00' )
GROUP BY blockNumber
ORDER BY total DESC
LIMIT 1;
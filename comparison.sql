Begin

-- creating a temporary table from the original data for further comparisons
CREATE TEMP TABLE origin AS
    SELECT * FROM data.top100numbersUsNames d
    WHERE EXISTS (
        SELECT Max(number) maxNum FROM data.top100numbersUsNames
        GROUP BY year
        HAVING maxNum = d.number);

-- Comparison by Count of Records
 SELECT COUNT(*) as CountRecords, "Origin Data" as Table FROM origin
 UNION ALL
 SELECT COUNT(*), "Result Data" FROM result.PopularNames100;

-- The Best way. Visual comparison by Row
 SELECT * FROM (
     origin o
     FULL OUTER JOIN
         (SELECT * FROM result.PopularNames100) r
         USING(year))
 WHERE TO_JSON_STRING(o) != TO_JSON_STRING(r)
 ORDER BY r.number DESC;

 -- Comparison by Number
WITH table_a AS (SELECT year, name, number FROM origin),
     table_b AS  (SELECT year, name, number FROM result.PopularNames100)
 SELECT * FROM
     (table_a FULL OUTER JOIN table_b
     ON table_a.year = table_b.year)
 WHERE table_a.number != table_b.number
     OR table_a.number IS NULL
     OR table_b.number IS NULL;

 -- comparison only by presence in both tables
 (SELECT * FROM origin
 UNION ALL
 SELECT * from result.PopularNames100)
 EXCEPT DISTINCT
 (SELECT * FROM origin
 INTERSECT DISTINCT
 SELECT * from result.PopularNames100);

End;
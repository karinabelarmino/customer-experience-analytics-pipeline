-- SQLite: runtime/cx_ex02.sqlite. Spark: substitute cx_ex02_<table> view names.
-- Main metric: count each response once at the fact's declared grain.
SELECT 100.0 * SUM(CASE WHEN score >= 9 THEN 1 WHEN score <= 6 THEN -1 ELSE 0 END)
       / NULLIF(COUNT(*), 0) AS nps
FROM responses
WHERE metric = 'NPS';

-- Use EXISTS (a semi-join) to filter by a category without multiplying responses.
SELECT COUNT(*) AS responses_in_atendimento,
       100.0 * SUM(CASE WHEN r.score >= 9 THEN 1 WHEN r.score <= 6 THEN -1 ELSE 0 END)
       / NULLIF(COUNT(*), 0) AS nps
FROM responses r
WHERE r.metric = 'NPS'
  AND EXISTS (
      SELECT 1 FROM response_categories c
      WHERE c.survey_id = r.survey_id AND c.response_id = r.response_id
        AND c.category = 'Atendimento'
  );

-- A response can have several subcategories under the same category.
-- Collapse to response/category BEFORE a main-category metric join.
WITH membership AS (
  SELECT DISTINCT survey_id, response_id, category FROM response_categories
)
SELECT c.category, COUNT(*) AS category_responses,
       100.0 * SUM(CASE WHEN r.score >= 9 THEN 1 WHEN r.score <= 6 THEN -1 ELSE 0 END)
       / NULLIF(COUNT(*), 0) AS category_nps
FROM responses r JOIN membership c
  ON c.survey_id = r.survey_id AND c.response_id = r.response_id
WHERE r.metric = 'NPS'
GROUP BY c.category;

-- Category populations overlap: their totals must not be added as unique people/responses.
-- COUNT(DISTINCT response_id) alone cannot repair an inflated SUM(score) numerator.
-- Response IDs are survey-scoped here: include BOTH key columns when deduplicating.

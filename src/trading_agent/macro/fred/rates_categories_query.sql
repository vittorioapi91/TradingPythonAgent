-- Example SQL query to get all branch categories
-- This file can be used with --categories-query-file argument

select series_id from

       (
           SELECT category_id 
    FROM category_paths
    WHERE full_path like '%Interest Rates%'
    group by category_id
) xx

    inner JOIN

(
    select category_id, series_id from series 
    where observation_end >= '2025-01-01'
) yy


on xx.category_id = yy.category_id

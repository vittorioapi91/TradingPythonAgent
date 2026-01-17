-- Analyze categories to identify leaves vs branches
-- A branch category has children (other categories with this category_id as parent_id)
-- A leaf category has no children

-- Option 1: Add is_branch, child_count, and parent_count columns to a view
CREATE OR REPLACE VIEW category_analysis AS
WITH RECURSIVE category_paths AS (
    -- Base case: root categories (parent_id is NULL or 0)
    SELECT 
        category_id,
        name,
        parent_id,
        ARRAY[category_id] AS path,
        0 AS depth
    FROM categories
    WHERE parent_id IS NULL OR parent_id = 0
    
    UNION ALL
    
    -- Recursive case: children with their parent paths
    SELECT 
        c.category_id,
        c.name,
        c.parent_id,
        cp.path || c.category_id,
        cp.depth + 1
    FROM categories c
    JOIN category_paths cp ON c.parent_id = cp.category_id
)
SELECT 
    c.category_id,
    c.name,
    c.parent_id,
    CASE 
        WHEN EXISTS (
            SELECT 1 FROM categories child 
            WHERE child.parent_id = c.category_id
        ) THEN true 
        ELSE false 
    END AS is_branch,
    (
        SELECT COUNT(*) 
        FROM categories child 
        WHERE child.parent_id = c.category_id
    ) AS child_count,
    COALESCE(cp.depth, 0) AS parent_count
FROM categories c
LEFT JOIN category_paths cp ON c.category_id = cp.category_id;

-- Query to see all categories with their branch/leaf status
SELECT 
    category_id,
    name,
    parent_id,
    is_branch,
    child_count,
    parent_count
FROM category_analysis
ORDER BY is_branch DESC, child_count DESC, parent_count DESC, name;

-- Count total branches and leaves
SELECT 
    COUNT(*) FILTER (WHERE is_branch = true) AS total_branches,
    COUNT(*) FILTER (WHERE is_branch = false) AS total_leaves,
    COUNT(*) AS total_categories
FROM category_analysis;

-- List only branch categories
SELECT 
    category_id,
    name,
    parent_id,
    child_count,
    parent_count
FROM category_analysis
WHERE is_branch = true
ORDER BY child_count DESC, parent_count DESC, name;

-- List only leaf categories
SELECT 
    category_id,
    name,
    parent_id,
    parent_count
FROM category_analysis
WHERE is_branch = false
ORDER BY parent_count DESC, name;


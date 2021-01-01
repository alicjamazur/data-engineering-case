-- Number of movies 
unload	
('SELECT company, COUNT(title)
 FROM avg_ratings
 GROUP BY company')
to 's3://ala-data/insights/number_of_movies_'
iam_role '<SUBST-ROLE-ARN>'
allowoverwrite;

-- Percent of Amazon movies on Netflix
unload
('SELECT ROUND(
  			CAST((SELECT COUNT(*)
	              FROM (SELECT * FROM avg_ratings WHERE company = \'amazon\') a
	              INNER JOIN (SELECT * FROM avg_ratings WHERE company = \'netflix\') b
	              ON a.title = b.title) AS DECIMAL(8, 2))                               
                  / CAST(COUNT(*) AS DECIMAL(8, 2))                           
             , 3) AS percent_of_amazon_on_netflix
FROM avg_ratings    
WHERE company = \'amazon\'')
to 's3://ala-data/insights/percent_of_amazon_on_netflix_'
iam_role '<SUBST-ROLE-ARN>'
allowoverwrite;

-- Global average movie rating
unload
('SELECT company, AVG(CAST(avg_rating AS DECIMAL(3,2))) AS global_avg_rating
FROM avg_ratings
GROUP BY company')
to 's3://ala-data/insights/global_avg_rating_'
iam_role '<SUBST-ROLE-ARN>'
allowoverwrite;

-- Most common release year
unload
('SELECT company, year
FROM   
       (SELECT year, company, COUNT(title) count, ROW_NUMBER() OVER (PARTITION BY company ORDER BY COUNT(year) DESC) rank
        FROM avg_ratings
        GROUP BY company, year)
WHERE rank = 1')
to 's3://ala-data/insights/most_common_release_year_'
iam_role '<SUBST-ROLE-ARN>'
allowoverwrite;

-- Best Amazon
unload
('SELECT a.title, a.avg_rating
FROM (SELECT * FROM avg_ratings WHERE company = \'amazon\') a
LEFT JOIN (SELECT * FROM avg_ratings WHERE company = \'netflix\') b
ON a.title = b.title
WHERE a.avg_rating >= 4 AND b.title IS NULL')          
to 's3://ala-data/insights/best_amazon_'
iam_role '<SUBST-ROLE-ARN>'
allowoverwrite;

-- Best Netflix
unload
('SELECT a.title, a.avg_rating
FROM (SELECT * FROM avg_ratings WHERE company = \'netflix\') a
LEFT JOIN (SELECT * FROM avg_ratings WHERE company = \'amazon\') b
ON a.title = b.title
WHERE a.avg_rating >= 4 AND b.title IS NULL')
to 's3://ala-data/insights/best_netflix_'
iam_role '<SUBST-ROLE-ARN>'
allowoverwrite;
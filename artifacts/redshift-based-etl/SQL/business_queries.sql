-- Number of movies 
unload	
('SELECT company, COUNT(title)
 FROM tmp_table
 GROUP BY company')
to 's3://ala-data/insights/number_of_movies_'
iam_role '<SUBST-ROLE-ARN>'
allowoverwrite;

-- Percent of Amazon movies on Netflix
unload
('SELECT ROUND((SELECT COUNT(*)
	           FROM (SELECT * FROM tmp_table WHERE company = \'Amazon\') a
	           INNER JOIN (SELECT * FROM tmp_table WHERE company = \'Netflix\') b
	           ON a.title = b.title) 
            
            / COUNT(*), 2) AS percent_of_amazon_available_on_netflix
FROM tmp_table    
WHERE company = \'Amazon\'')
to 's3://ala-data/insights/percent_of_amazon_on_netflix_'
iam_role '<SUBST-ROLE-ARN>'
allowoverwrite;

-- Global average movie rating
unload
('SELECT company, ROUND(AVG(avg_rating), 2) AS global_avg_rating
FROM tmp_table
GROUP BY company')
to 's3://ala-data/insights/global_avg_rating_'
iam_role '<SUBST-ROLE-ARN>'
allowoverwrite;

-- Most common release year
unload
('SELECT company, year
FROM   
       (SELECT year, company, COUNT(title) count, ROW_NUMBER() OVER (PARTITION BY company ORDER BY COUNT(year) DESC) rank
        FROM tmp_table
        GROUP BY company, year)
WHERE rank = 1')
to 's3://ala-data/insights/most_common_release_year_'
iam_role '<SUBST-ROLE-ARN>'
allowoverwrite;

-- Best Amazon
unload
('SELECT a.title, a.avg_rating
FROM (SELECT * FROM tmp_table WHERE company = \'Amazon\') a
LEFT OUTER JOIN (SELECT * FROM tmp_table WHERE company = \'Netflix\') b
ON a.title = b.title
WHERE a.avg_rating >= 4')             
to 's3://ala-data/insights/best_amazon_'
iam_role '<SUBST-ROLE-ARN>'
allowoverwrite;

-- Best Netflix
unload
('SELECT a.title, a.avg_rating
FROM (SELECT * FROM tmp_table WHERE company = \'Netflix\') a
LEFT OUTER JOIN (SELECT * FROM tmp_table WHERE company = \'Amazon\') b
ON a.title = b.title
WHERE a.avg_rating >= 4')
to 's3://ala-data/insights/best_netflix_'
iam_role '<SUBST-ROLE-ARN>'
allowoverwrite;
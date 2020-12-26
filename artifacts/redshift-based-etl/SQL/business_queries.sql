# Number of movies 
unload	(SELECT COUNT(title)
		 FROM tmp_table
		 GROUP BY company)
to 's3://ala-data/insights/number_of_movies_' 

# Percent of Amazon movies on Netflix
unload
(SELECT ROUND(  
               (SELECT COUNT(*)
                FROM (SELECT * FROM tmp_table WHERE company = 'Amazon') a
                INNER JOIN (SELECT * FROM tmp_table WHERE company = 'Netflix') b
                ON a.title = b.title) 
                
                / COUNT(*), 2) AS percent_of_amazon_available_on_netflix
FROM tmp_table    
WHERE company = 'Amazon')
to 's3://ala-data/insights/percent_of_amazon_on_netflix_' 

# Global Average Rating
unload
(SELECT company, ROUND(AVG(avg_rating), 2) AS global_avg_rating
FROM tmp_table
GROUP BY company)
to 's3://ala-data/insights/global_avg_rating_' 

# Most common release year
unload
(SELECT company, year, count
FROM   
       (SELECT year, company, COUNT(title) count, ROW_NUMBER() OVER (PARTITION BY company ORDER BY COUNT(year) DESC) rank
        FROM tmp_table
        GROUP BY company, year)
WHERE rank = 1)
to 's3://ala-data/insights/most_common_release_year_' 

# Best Amazon
unload
(SELECT title, avg_rating
FROM (SELECT * FROM tmp_table WHERE company == 'Amazon') a
LEFT ANTI JOIN (SELECT * FROM tmp_table WHERE company == 'Netflix') b
ON a.title = b.title
WHERE avg_rating >= 4)             
to 's3://ala-data/insights/best_amazon_' 

# Best Netflix
unload
(SELECT title, avg_rating
FROM (SELECT * FROM tmp_table WHERE company == 'Netflix') a
LEFT ANTI JOIN (SELECT * FROM tmp_table WHERE company == 'Amazon') b
ON a.title = b.title
WHERE avg_rating >= 4)
to 's3://ala-data/insights/best_netflix_' 
INSERT INTO ratings
SELECT * 
FROM ratings.ratings;


INSERT INTO avg_ratings
SELECT AVG(CAST(rating AS decimal(3, 2))) AS avg_rating, year, title, company 
FROM ratings.ratings 
GROUP BY company, title, year;
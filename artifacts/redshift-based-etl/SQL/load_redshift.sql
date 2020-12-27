INSERT INTO ratings
SELECT * FROM ratings.ratings;


INSERT INTO tmp_table
SELECT AVG(rating) as avg_rating, year, title, company FROM ratings.ratings GROUP BY company, title, year;
-- a. Number of unique film titles
SELECT 
	COUNT(DISTINCT(title)) AS num_of_unique_film_titles
FROM 
	movies;

-- b. Film Title, Year of Release, and Rating of the film starring Lena Headey, 
-- Sort By Year of Release.
WITH movie_star_table AS (
	SELECT 
		ms.movie_id, 
		ms.person_id,  
		p.person_name
	FROM 
		movies_star AS ms
	JOIN 
		person AS p 
		ON ms.person_id = p.person_id
	WHERE 
		p.person_name = 'Lena Headey' -- Hardcoded variable
),
rating_and_person_table AS (
	SELECT 
		mi.movie_id, 
		mi.rating, 
		mst.person_id,  
		mst.person_name
	FROM 
		movies_information AS mi
	JOIN 
		movie_star_table AS mst
		ON mi.movie_id = mst.movie_id
)
SELECT 
	m.title, 
	m.release_year, 
	rpt.rating
FROM 
	movies AS m
JOIN 
	rating_and_person_table AS rpt
	ON m.movie_id = rpt.movie_id
ORDER BY 
	m.release_year;

-- c. The name of the director and total gross of the films that have been directed.
WITH movie_director_table AS (
	SELECT 
		md.movie_id, 
		md.person_id,  
		p.person_name
	FROM 
		movies_director AS md
	JOIN 
		person AS p
		ON md.person_id = p.person_id
	WHERE 
		p.person_name IS NOT NULL
)
SELECT 
	mdt.person_name AS director, m.gross
FROM 
	movie_director_table AS mdt
JOIN 
	movies AS m
	ON mdt.movie_id = m.movie_id
WHERE 
	m.gross IS NOT NULL
GROUP BY 
	mdt.person_name, m.gross;

-- d. Film Title, Year of Release, and Rating of 5 films 
-- that have comedy genre with the largest gross
WITH genre_comedy AS (
	SELECT 
		mg.movie_id, 
		mg.genre_id, 
		g.genre_name
	FROM 
		movies_genre AS mg
	JOIN 
		genre AS g
		ON mg.genre_id = g.genre_id
	WHERE 
		g.genre_name = 'Comedy' -- Hardcoded Variable
),
genre_and_rating_table AS (
	SELECT 
		mi.movie_id, 
		mi.rating, 
		gc.genre_name
	FROM 
		movies_information AS mi
	JOIN 
		genre_comedy AS gc
		ON mi.movie_id = gc.movie_id
)
SELECT 
	m.title, m.release_year, gst.rating, m.gross
FROM 
	movies AS m
JOIN 
	genre_and_rating_table AS gst
	ON m.movie_id = gst.movie_id
WHERE 
	m.gross IS NOT NULL
ORDER BY 
	m.gross DESC
LIMIT 
	5;

-- e. Film Title, Year of Release and Rating of the film 
-- directed by Martin Scorsese and starring Robert De Niro
WITH movie_star_table AS (
	SELECT 
		ms.movie_id, 
		ms.person_id,  
		p.person_name AS star_name
	FROM 
		movies_star AS ms
	JOIN 
		person AS p
		ON ms.person_id = p.person_id
	
	WHERE 
		p.person_name = 'Robert De Niro'  -- Hardcoded variable
),
movie_director_table AS (
	SELECT 
		ms.movie_id, 
		ms.person_id,  
		p.person_name AS director_name
	FROM 
		movies_director AS ms
	JOIN 
		person AS p
		ON ms.person_id = p.person_id
	WHERE 
		p.person_name = 'Martin Scorsese'  -- Hardcoded variable
),
director_and_star_table AS (
	SELECT 
		mdt.movie_id, 
		mdt.director_name, 
		mst.star_name
	FROM 
		movie_director_table AS mdt
	JOIN 
		movie_star_table AS mst
		ON mdt.movie_id = mst.movie_id
)
SELECT 
	m.title, 
	m.release_year, 
	mi.rating
FROM 
	director_and_star_table AS dst
JOIN 
	movies_information AS mi
	ON dst.movie_id = mi.movie_id
JOIN 
	movies AS m
	ON dst.movie_id = m.movie_id;

-- Refer to ER Diagram (ER_Diagram.png) for table creation
CREATE TABLE movies(
	movie_id INT,
	title VARCHAR,
	release_year INT,
	gross REAL
);

CREATE TABLE genre(
	genre_id INT,
	genre_name VARCHAR
);

CREATE TABLE person(
	person_id INT,
	person_name VARCHAR
);

CREATE TABLE movies_information(
	info_id INT,
	movie_id INT,
	runtime INT,
	rating REAL,
	votes_count INT,
	overview TEXT
);

CREATE TABLE movies_genre(
	movie_id INT,
	genre_id INT
);

CREATE TABLE movies_director(
	director_id INT,
	movie_id INT,
	person_id INT
);

CREATE TABLE movies_star(
	star_id INT,
	movie_id INT,
	person_id INT
);

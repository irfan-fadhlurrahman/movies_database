-- Refer to ER Diagram for table creation
CREATE TABLE movies(
	movie_id INT,
	title VARCHAR(255),
	release_year INT,
	gross REAL
);

CREATE TABLE genre(
	genre_id INT,
	genre_name VARCHAR(255)
);

CREATE TABLE person(
	person_id INT,
	person_name VARCHAR(255)
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

-- Add constraints
-- Primary Keys
ALTER TABLE movies ADD PRIMARY KEY (movie_id);
ALTER TABLE genre ADD PRIMARY KEY (genre_id);
ALTER TABLE person ADD PRIMARY KEY (person_id);
ALTER TABLE movies_information ADD PRIMARY KEY (info_id);
ALTER TABLE movies_director ADD PRIMARY KEY (director_id);
ALTER TABLE movies_star ADD PRIMARY KEY (star_id);

-- Foreign Keys
ALTER TABLE movies_information ADD FOREIGN KEY (movie_id) REFERENCES movies(movie_id);
ALTER TABLE movies_genre ADD FOREIGN KEY (movie_id) REFERENCES movies(movie_id);
ALTER TABLE movies_genre ADD FOREIGN KEY (genre_id) REFERENCES genre(genre_id);
ALTER TABLE movies_director ADD FOREIGN KEY (movie_id) REFERENCES movies(movie_id);
ALTER TABLE movies_director ADD FOREIGN KEY (person_id) REFERENCES person(person_id);
ALTER TABLE movies_star ADD FOREIGN KEY (movie_id) REFERENCES movies(movie_id);
ALTER TABLE movies_star ADD FOREIGN KEY (person_id) REFERENCES person(person_id);
## Code Test - Data Engineer

Here are the tools and libraries that were used for this code test.
* Programming Language: Python 3.8, SQL
* Relational database: PostgreSQL 14, pgAdmin 4
* Libraries: Regex, Pandas, Numpy, SQLAlchemy, Psycopg2-binary

### Background

Raw dataset, [movies.csv](https://github.com/irfan-fadhlurrahman/movies_database/blob/main/movies.csv) contains information about movies that include films and tv shows. This raw dataset have the following columns such as:
* **MOVIES**: the title of a film or a tv show.
* **YEAR**: release year or end year of movies.
* **GENRE**: a movie category that can be more than one.
* **RATING**: a rating of the movie.
* **ONE-LINE**: an overview of the movie.
* **STARS**: a director, a star, or both of the movie. Both director and star can be more than one.
* **VOTES**: total votes count of the movie.
* **RunTime**: total duration of the movie.
* **Gross**: Total revenue of the movie.

From this raw dataset, we have to:
1. Create a data model to describe entities, attributes, and relationships.
2. Create a database based on the data model that has been created.
3. Create an ETL pipeline to collect, transform, and ingest the data into the created database.
4. Create a query to display the following items:
    * Number of unique film titles
    * Film Title, Year of Release, and Rating of the film starring Lena Headey Sort By Year of Release.
    * The name of the director and total gross of the films that have been directed.
    * Film Title, Year of Release, and Rating of 5 films that have comedy genre with the largest gross.
    * Film Title, Year of Release and Rating of the film directed by Martin Scorsese and starring Robert De Niro.
    
### Data Model

The data model as follows:

**Conceptual Data Model**

![alt text](https://github.com/irfan-fadhlurrahman/movies_database/blob/main/er_diagram/conceptual_erd.png)

**Logical Data Model**

![alt text](https://github.com/irfan-fadhlurrahman/movies_database/blob/main/er_diagram/logical_erd.png)

**Physical Data Model**

![alt text](https://github.com/irfan-fadhlurrahman/movies_database/blob/main/er_diagram/physical_erd.png)

The entities that are used to create a data model as follows:
1. **movies**:
   This entity contains attributes such as **movie_id, title, release_year, and gross**. The reason to store all attributes in this entity because a movie can only have one title, one release year, and one gross.

2. **genre**:
   Genre's attributes are **genre_id and genre_name**. A one movie can have multiple genre, so it is better to store their id and their unique values in the separate entity.

3. **person**:
   Person's attributes are **person_id** and **person_name**. This table is created to avoid duplication of a director and a star that could be the same for a movie.

4. **movies_information**:
   This entity contains **info_id**, **movie_id**, **runtime**, **rating**, **votes_count**, and **overview**. A movie can have all attributes more than one because there is tv shows.
   
5. **movies_genre**:
   This entity contains **movie_id** and **genre_id** to connect the relationship between movies entity and genre entity.
   
6. **movies_director**:
   This entity contains **director_id**, **movie_id**, and **person_id** to connect the relationship between person entity and movie entity.
   
6. **movies_star**:
   This entity contains **star_id**, **movie_id**, and **person_id** to connect the relationship between person entity and movie entity.
   
### Database Creation

The database are created by using **CREATE TABLE** and **ALTER TABLE** (for constraints) statement in the PostgreSQL. The query is at [create_table_with_constraints.sql](https://github.com/irfan-fadhlurrahman/movies_database/blob/main/query_script/create_table_with_constraints.sql).

### ETL Pipeline

There are five scripts to build the ETL pipeline for importing the movies.csv dataset into created database such
* [database_credentials.py](https://github.com/irfan-fadhlurrahman/movies_database/blob/main/database_credentials.py): To create an engine for database connection to Python and to define absolute path of a folder and the data type of each attributes for ingestion with pandas.
* [cleaning.py](https://github.com/irfan-fadhlurrahman/movies_database/blob/main/cleaning.py): To perform data cleaning on the dataset.
* [transform.py](https://github.com/irfan-fadhlurrahman/movies_database/blob/main/transform.py): To separate the cleaned dataset into seven dataset for storing in the relational database.
* [ingest.py](https://github.com/irfan-fadhlurrahman/movies_database/blob/main/ingest.py): To ingest all of the seven dataset into the created database.
* [etl.py](https://github.com/irfan-fadhlurrahman/movies_database/blob/main/etl.py): To run all four scripts above as one script.

The step-by-step of ETL pipeline as follows:
1. Extract
    * Download the movies.csv from Google Drive then read it as dataframe in Python.

2. Transform
    * Clean the dataset to remove duplication, remove unnecessary character in the rows of dataframe, and rename the columns.
    * Separate clean dataset into seven tables as per created data model.

3. Load
   * Build a connection to postgres database then ingest each table by using pandas.

### Query
All necessary data have been imported to the created database. The following below are the query for specific tasks. You can run the query with this file [here](https://github.com/irfan-fadhlurrahman/movies_database/blob/main/query_script/query.sql).

#### a. Number of unique film titles
Query results: [Task_4a.csv](https://github.com/irfan-fadhlurrahman/movies_database/blob/main/query_result/Task_4a.csv)
```
SELECT COUNT(DISTINCT(title)) AS num_of_unique_film_titles
FROM movies;
```

#### b. Film Title, Year of Release, and Rating of the film starring Lena Headey Sort By Year of Release
Query results: [Task_4b.csv](https://github.com/irfan-fadhlurrahman/movies_database/blob/main/query_result/Task_4b.csv)
```
-- First Join
WITH movie_star_table AS (
	SELECT ms.movie_id, ms.person_id,  p.person_name
	FROM movies_star AS ms
	JOIN person AS p
		ON ms.person_id = p.person_id
    -- Hardcoded variable
	WHERE p.person_name = 'Lena Headey'
),
-- Second Join
rating_and_person_table AS (
	SELECT mi.movie_id, mi.rating, mst.person_id,  mst.person_name
	FROM movies_information AS mi
	JOIN movie_star_table AS mst
		ON mi.movie_id = mst.movie_id
)
-- Third join
SELECT m.title, m.release_year, rpt.rating
FROM movies AS m
JOIN rating_and_person_table AS rpt
	ON m.movie_id = rpt.movie_id
ORDER BY m.release_year;
```
#### c. The name of the director and total gross of the films that have been directed
Query results: [Task_4c.csv](https://github.com/irfan-fadhlurrahman/movies_database/blob/main/query_result/Task_4c.csv)
```
-- First Join
WITH movie_director_table AS (
	SELECT md.movie_id, md.person_id,  p.person_name
	FROM movies_director AS md
	JOIN person AS p
		ON md.person_id = p.person_id
	WHERE p.person_name IS NOT NULL
)
SELECT mdt.person_name AS director, m.gross
FROM movie_director_table AS mdt
JOIN movies AS m
	ON mdt.movie_id = m.movie_id
WHERE m.gross IS NOT NULL
GROUP BY mdt.person_name, m.gross;
```

#### d. Film Title, Year of Release, and Rating of 5 films that have comedy genre with the largest gross
Query results: [Task_4d.csv](https://github.com/irfan-fadhlurrahman/movies_database/blob/main/query_result/Task_4d.csv)
```
WITH genre_comedy AS (
	SELECT mg.movie_id, mg.genre_id, g.genre_name
	FROM movies_genre AS mg
	JOIN genre AS g
		ON mg.genre_id = g.genre_id
	WHERE g.genre_name = 'Comedy' -- Hardcoded Variable
),
genre_and_rating_table AS (
	SELECT mi.movie_id, mi.rating, gc.genre_name
	FROM movies_information AS mi
	JOIN genre_comedy AS gc
		ON mi.movie_id = gc.movie_id
)
SELECT m.title, m.release_year, gst.rating, m.gross
FROM movies AS m
JOIN genre_and_rating_table AS gst
	ON m.movie_id = gst.movie_id
WHERE m.gross IS NOT NULL
ORDER BY m.gross DESC
LIMIT 5;
```

#### e. Film Title, Year of Release and Rating of the film directed by Martin Scorsese and starring Robert De Niro
Query results: [Task_4e.csv](https://github.com/irfan-fadhlurrahman/movies_database/blob/main/query_result/Task_4e.csv)
```
-- First Join
WITH movie_star_table AS (
	SELECT ms.movie_id, ms.person_id,  p.person_name AS star_name
	FROM movies_star AS ms
	JOIN person AS p
		ON ms.person_id = p.person_id
	-- Hardcoded variable
	WHERE p.person_name = 'Robert De Niro'
),
movie_director_table AS (
	SELECT ms.movie_id, ms.person_id,  p.person_name AS director_name
	FROM movies_director AS ms
	JOIN person AS p
		ON ms.person_id = p.person_id
	-- Hardcoded variable
	WHERE p.person_name = 'Martin Scorsese'
),
director_and_star_table AS (
	SELECT mdt.movie_id, mdt.director_name, mst.star_name
	FROM movie_director_table AS mdt
	JOIN movie_star_table AS mst
		ON mdt.movie_id = mst.movie_id
)
SELECT m.title, m.release_year, mi.rating, dst.director_name, dst.star_name
FROM director_and_star_table AS dst
JOIN movies_information AS mi
	ON dst.movie_id = mi.movie_id
JOIN movies AS m
	ON dst.movie_id = m.movie_id
```

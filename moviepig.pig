movies = LOAD '/user/maria_dev/ml-latest/data/movies.csv'
            USING org.apache.pig.piggybank.storage.CSVExcelStorage(',', 'NO_MULTILINE',
            'NOCHANGE', SKIP_INPUT_HEADER') AS (movieId:int, title:chararry, genres:chararray);

ratings_orig = LOAD '/user/maria_dev/ml-latest/data/ratings.csv'
                USING PigStorage(',') AS (userId:int, movieId:int, rating:float, timestamp:chararray);

ratings = FILTER ratings_orig BY $0 is not null;
grouped = GROUP ratings BY movieId;
counted = FOREACH ground GENERATE FLATTEN(group) AS (movieId), AVG(ratings.ratings) AS avgr,
            COUNT(ratings.rating) AS counted
joined = JOIN movies BY (movieId), counted BY (movieId);
sorted = ORDER joined BY cnt DESC;
result = FOREACH sorted GENERATE title AS (title), avgr AS (avgr), cnt AS (cnt);
filtered = FILTER result BY avgr<2.0;
final = LIMIT filtered 30;
DUMP final;
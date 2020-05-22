ratings = Load '/user/maria_dev/ml-100k/u.data' AS (userID:int, movieID:int, rating:int, ratingTime:int);

metadata = Load '/user/maria_dev/ml-100k/u.item' USING PigStorage('|') AS (movieID:int, movieTitle:chararray, releaseDate:chararray, videoRelease:chararray, imdbLink:chararray);

nameLookup = FOREACH metadata GENERATE movieID, movieTitle;

ratingsByMovie = Group ratings by movieID;

avgRatings = FOREACH ratingsByMovie GENERATE group AS movieID, AVG(ratings.rating) AS avgRating, COUNT(ratings.rating) as countRating;

oneStarMovies = FILTER avgRatings by avgRating < 2.0;
	
mergeData = JOIN oneStarMovies BY movieID, nameLookup BY movieID;

finalData = FOREACH mergeData GENERATE nameLookup::movieTitle AS movieName, oneStarMovies::avgRating AS Rating, oneStarMovies::countRating as totalviews;

finalDataSorted = ORDER finalData by totalviews DESC;

DUMP finalDataSorted
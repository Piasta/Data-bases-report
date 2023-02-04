/*FOR pigv-x local ...*/
ratings = LOAD '/home/sshuser/PiotrPiasta/Pig/ratings.csv' USING PigStorage(',') AS (userId:int, movieId:int, rating:float, timestamp:long);

/*For pig -x mapreduce ...*/
ratings = LOAD '/user/ratings.csv' USING PigStorage(',') AS (userId:int, movieId:int, rating:float, timestamp:long);

grouped_ratings = GROUP ratings BY movieId;
average_ratings = FOREACH grouped_ratings GENERATE group as movieId, AVG(ratings.rating) as avg_rating;

DUMP average_ratings;
STORE average_ratings INTO '/home/sshuser/PiotrPiasta/Pig/Output' USING PigStorage(',');

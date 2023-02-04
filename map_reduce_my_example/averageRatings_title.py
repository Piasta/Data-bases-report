from mrjob.job import MRJob


class AverageMovieRatings(MRJob):

    def mapper(self, _, line):
        data = line.split(',')
        if data[0] == 'userId':
            return
        try:
            userId, movieId, rating, timestamp = data
            rating = float(rating)
        except ValueError:
            return
        yield int(movieId), (rating,1)

    def reducer(self, movieId, ratings_counts):
        total_ratings = 0
        total_count = 0
        for rating, count in ratings_counts:
            total_ratings += rating
            total_count += count

        import csv

        with open('C:/Users/ppiasta/PycharmProjects/hadoop-master/map_reduce_my_example/movies.csv', newline='', encoding='utf-8') as f:
            reader = csv.reader(f)
            movies = {row[0]: row[1] for row in reader}

        yield movies[str(movieId)], total_ratings / total_count


if __name__ == '__main__':
    AverageMovieRatings.run()

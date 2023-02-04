from mrjob.job import MRJob


class AverageMovieRatings(MRJob):

    def mapper(self, _, line):
        data = line.split(',')
        if data[0] == 'userId':
            return
        userId, movieId, rating, timestamp = data
        yield int(movieId), (float(rating), 1)

    def reducer(self, movieId, ratings_counts):
        total_ratings = 0
        total_count = 0
        for rating, count in ratings_counts:
            total_ratings += rating
            total_count += count
        yield movieId, total_ratings / total_count


if __name__ == '__main__':
    AverageMovieRatings.run()
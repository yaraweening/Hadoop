class RatingsBreakdown (MRJob):
    def steps(self):
        return [
            MRStep(mapper=self.mapper_get_ratings, combiner=self.combine_count_ratings,
                   reducer=self.reducer_count_ratings),
            MRStep(reducer=self.reduce_sort_counts)
        ]

    def mapper_get_ratings(self, _, line):
        (userID, movieID, rating, timestamp) = line.split('\t')
        yield rating, 1

    def combine_count_ratings(self, movie, counts):
        yield movie, sum(counts)

    def reducer_count_ratings (self, key, values):
        yield None, (sum(values), key)

    def reduce_sort_counts(self, _, movie_counts):
        for count, key in sorted(movie_counts, reverse=True):
            yield ('%d' % int(count), key)

if __name__ == '__main__':
    RatingsBreakdown.run()
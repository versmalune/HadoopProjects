from mrjob.job import MRJob
from mrjob.step import MRStep
import csv

class AverageRating(MRJob):
    def steps(self):
        return [
            MRStep(mapper=self.map_rating_count),
                    reducer=self.reduce_rating_count),
            MRStep(reducer=self.reduce_sort)
        ]
    
    def map_rating_count(self, _, line):
        data = [a.strip() for a in csv.reader([line]).next()]
        if len(data) == 4:
            if data[0] != 'userId': yield data[1], float(data[2])
        elif len(data) == 3:
            if data[0] != 'movieId': yield data[0], data[1]

    def reduce_rating_count(self, movieId, value):
        rating = 0.0
        count = 0.0
        for v in value:
            if ininstance(v, basestring):
                title = v
            else:
                rating += v
                count += 1
        if count >= 100:
            yield rating/count, title
    
    def reduce_sort(self, avg, movieId):
        for m in movieId:
            yield '{:4.3f}'.format(avg), m

if __name__ == '__main__':
    AverageRating.run()
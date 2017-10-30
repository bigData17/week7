from mrjob.job import MRJob; from mrjob.step import MRStep; import re

WORD_RE = re.compile(r"[\w']+")
class MRWordOccurrance(MRJob):
    def steps(self):
        return [
            MRStep(mapper=self.mapper_get_words,
                   combiner=self.combiner_count_words,
                   reducer=self.reducer_count_words)]

    def mapper_get_words(self, _, line):            # yield each word in the line
        for word in WORD_RE.findall(line):
            yield (word.lower(), 1)                 # yields all words in lowercase
    def combiner_count_words(self, word, counts):   # sum the words we've seen
        yield (word, sum(counts))
    def reducer_count_words(self, word, counts):    # send all pairs to the same reducer.
        yield (sum(counts), word)

if __name__ == '__main__':
    MRWordOccurrance.run()
    
from mrjob.job import MRJob
from mrjob.step import MRStep
import re
 
#wc_regex = re.compile(r"['\w']+")
wc_regex = re.compile(r"\w+")
 
class MRWord_freq_count(MRJob):
 
    def mapper_get_words(self, key, line):
 
        words = wc_regex.findall(line)
        for word in words:
            yield word.lower(), 1
 
    def reducer_count_words(self, word, values):
        ''' sum up count of each words'''
        yield word, sum(values)
 
    def mapper_count_keys(self, word, count):
        yield int(count)%2, word
 
    def reducer_output_words(self, count, words):
        for word in words:
            yield None, (  count, 'Euler' )

    # discard the key; it is just None
    def out(self, _, node_count_pairs):
        # each item of word_count_pairs is (count, 'Euler'),
        # so yielding one results in key=counts, value=word
        yield max(node_count_pairs)
 
    def steps(self):
        return  [
            MRStep(mapper = self.mapper_get_words, 
                   reducer = self.reducer_count_words),
            MRStep(mapper = self.mapper_count_keys, 
                   reducer = self.reducer_output_words)
            ,MRStep( reducer = self.out)
            ]
 
if __name__ == '__main__':
    MRWord_freq_count.run()
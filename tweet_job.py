from mrjob.job import MRJob
from mrjob.step import MRStep

class tweet_job(MRJob):
    
    def map(self, _, tweet):
        
        # This will keep track of how many filtered tweets we process
        self.increment_counter('group', 'tweet_count', 1)
        
        # Initializing and yielding our tags
        num_smiley = sum(1  for i in tweet.split() if i.startswith(":)"))
        num_sad = sum(1 for i in tweet.split() if i.startswith(":(")) 
        num_hash = sum(1 for i in tweet.split() if i.startswith("#"))
        num_ats = sum(1 for i in tweet.split() if i.startswith("@"))
        yield ":)", num_smiley
        yield ":(", num_sad
        yield "#", num_hash
        yield "@", num_ats
 
    def combiner(self, tag, num_count):
        yield (tag, sum(num_count)) 

    def reduce(self, tag, num_count):
        yield None, (sum(num_count), tag)
    
    def steps(self):
        return [
                # This line below filters out lines without "JohnSnow07"
            MRStep(mapper_pre_filter='grep "JohnSnow07"',
                   mapper=self.map,
                   combiner=self.combiner,
                   reducer=self.reduce)
            # If multistep, use as below
            #MRStep(reducer=self.reduce2)
        ]

if __name__ == '__main__':
    tweet_job.run()

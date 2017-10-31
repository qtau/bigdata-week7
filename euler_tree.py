from mrjob.job import MRJob
from mrjob.step import MRStep

def node_participation(tree_line):
    node_edge_participation = tree_line.split()
    return(node_edge_participation)

def is_even(counts):
    if counts % 2 == 0:
        result = "even"
    else:
        result = "odd"
    return(result)

     
class MREulerGraph(MRJob):
    
    def steps(self):
            return [
                MRStep(mapper=self.mapper_get_nodes,
                       combiner=self.combiner_count_nodes,
                       reducer=self.reducer_count_nodes),
                MRStep(mapper = self.mapper_get_even_behavior,
                       combiner=self.combiner_find_odd_nodes,
                       reducer=self.reducer_find_odd_nodes)
            ]

    def mapper_get_nodes(self, _, tree_line):
        for node in node_participation(tree_line):
            yield (node, 1)

    def combiner_count_nodes(self, node, counts):
        # optimization: sum the words we've seen so far
        yield (node, sum(counts))

    def reducer_count_nodes(self, node, counts):
        yield node, sum(counts)

    def mapper_get_even_behavior(self,node,counts):
        yield (is_even(counts), 1)

    def combiner_find_odd_nodes(self, is_even, counts):
        yield is_even, sum(counts)
        
    def reducer_find_odd_nodes(self, is_even, counts):
        yield is_even, sum(counts)


if __name__ == '__main__':
    MREulerGraph.run()

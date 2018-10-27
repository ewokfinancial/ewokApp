from collections import deque
import itertools
import csv

class Pipeline:
    """
    The Pipeline class provides an object that schedules tasks
    and executes them

    Attributes:
        tasks: a directed acyclic graph (DAG) that keeps track of 
                each task and its dependants
    """
    def __init__(self):
        """Initializes tasks with a DAG object"""
        self.tasks = DAG()
        
    def task(self, depends_on=None):
        """
        This is a decorator method that adds tasks to the pipeline.
        It adds a root node if it has no dependencies, otherwise 
        it also adds the edge to the node it depends on.

            depends_on: function, a task the current task depends on
        """
        def inner(f):
            """
            f: function, the task that is currently being added
                 to the pipeline
            """
            self.tasks.add(f)
            if depends_on:
                self.tasks.add(depends_on, f)
            return f
        return inner
    
    def run(self):
        """
        Executes the tasks according to the DAG scheduler

        returns the completed dictionary with every task:output
        """
        # list of topologically sorted nodes.
        scheduled = self.tasks.sort()
        # dictionary using task:output pairs.
        completed = {}

        for task in scheduled:
            # the dag stores node:values pairs, where values
            # is a list of tasks dependant on that node
            for node, dependants in self.tasks.graph.items():
                if task in dependants:
                    # if the task depends on the node use 
                    # its output as the input of the task
                    completed[task] = task(completed[node])

            if task not in completed:
                # task has no dependencies
                completed[task] = task()
                
        return completed

class DAG:
    """
    The DAG class creates a directed acyclic graph data structure
    to serve as the scheduler for the Pipeline class.
    """
    def __init__(self):
        """
        Initializes an empty graph that will store
        node:[list of dependants] pairs
        """
        self.graph = {}

    def add(self, node, to=None):
        """
        Adds nodes and its dependants (if existant) to the graph

        node: function, the task being added
        to: None or function, a node that depends on the
            current task (node) or None if it doesn't have
            dependants
        """
        if not node in self.graph:
            self.graph[node] = []
        if to:
            if not to in self.graph:
                self.graph[to] = []
            self.graph[node].append(to)
        # Add validity check. 
        # sorted_nodes will be longer than the graph
        # if the graph has a cycle 
        sorted_nodes = self.sort()
        if len(sorted_nodes) != len(self.graph):
            print("The graph is not acyclical")
            raise Exception

    def sort(self):
        """
        sorts the graph topologically, where importance is measured
        using in-degrees. Node importance increases as it's number 
        of in-degrees decrease. 

        returns a list of the sorted nodes
        """
        # get the in-degrees of each node
        in_degrees = self.in_degrees()
        # initialize to_visit as a deque (double-ended queue),
        # which supports adding and removing elements from either end.
        to_visit = deque()

        # find the root nodes in the DAG and add them to to_visit. 
        # root nodes have zero in-degrees.
        for node in in_degrees:
            if in_degrees[node] == 0:
                to_visit.append(node)
                
        # search is a list of sorted nodes by dependency         
        searched = []
        # while loop will break when the to_visit deque is empty
        while to_visit:
            # pops the first value from to_visit into node
            node = to_visit.popleft()
            # advance the graph to the next nodes
            for pointer in self.graph[node]:
                in_degrees[pointer] -= 1
                if in_degrees[pointer] == 0:
                    to_visit.append(pointer)
            searched.append(node)
            
        return searched

    def in_degrees(self):
        """
        Builds and returns a dictionary with node:in-degrees pairs
        """
        in_degrees = {}
        for node, edges in self.graph.items():
            # each node starts with an in-degree of zero
            # and increases by one each time it appears in "edges"
            if node not in in_degrees:
                in_degrees[node] = 0
            for edge in edges:
                if edge not in in_degrees:
                    in_degrees[edge] = 0
                in_degrees[edge] += 1
                
        return in_degrees


def build_csv(lines, header=None, file=None):
    def open_file(f):
        # If it's a string, then open the file
        # and return the opened file.
        if isinstance(f, str):
            f = open(f, 'w')
        return f

    file = open_file(file)  # add inner function.
    if header:
        lines = itertools.chain([header], lines)
    writer = csv.writer(file, delimiter=',')
    writer.writerows(lines)
    file.seek(0)
    return file
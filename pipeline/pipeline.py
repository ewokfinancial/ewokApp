from collections import deque
import itertools
import csv

class Pipeline:
    def __init__(self):
        self.tasks = DAG()
        
    def task(self, depends_on=None):
        """
        This is a decorator method that adds tasks to the pipeline
        depends_on: function, a task the current task depends on
        """
        def inner(f):
            self.tasks.add(f)
            if depends_on:
                self.tasks.add(depends_on, f)
            return f
        return inner
    
    def run(self):
        # scheduled is a list of topologically sorted nodes.
        scheduled = self.tasks.sort()
        """
        the completed dictionary uses a task: output
        pairs that can be used for other tasks that are dependant
        on the given task
        """
        completed = {}
        for task in scheduled:
            for node, values in self.tasks.graph.items():
                if task in values:
                    completed[task] = task(completed[node])
            if task not in completed:
                completed[task] = task()
                
        return completed

class DAG:
    def __init__(self):
        self.graph = {}

    def add(self, node, to=None):
        if not node in self.graph:
            self.graph[node] = []
        if to:
            if not to in self.graph:
                self.graph[to] = []
            self.graph[node].append(to)
        # Add validity check.
        sorted_nodes = self.sort()
        if len(sorted_nodes) != len(self.graph):
            print("The graph is not acyclical")
            raise Exception

    def sort(self):
        in_degrees = self.in_degrees()
        to_visit = deque()
        # find the root nodes in the DAG
        for node in in_degrees:
            if in_degrees[node] == 0:
                to_visit.append(node)
                
        # initialize list of sorted nodes by dependency         
        searched = []
        # while loop will break when the to_visit deque is empty
        while to_visit:
            node = to_visit.popleft()
            for pointer in self.graph[node]:
                in_degrees[pointer] -= 1
                if in_degrees[pointer] == 0:
                    to_visit.append(pointer)
            searched.append(node)
            
        return searched

    def in_degrees(self):
        in_degrees = {}
        for node, edges in self.graph.items():
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

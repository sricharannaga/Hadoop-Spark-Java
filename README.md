# Hadoop-Spark-Java
A collection of java related files and code for processing of big data using apache hadoop spark, pig platforms

For twitter.java the following is the constraint 
In this project, you are asked to implement a simple graph algorithm that needs two Map-Reduce 
jobs. You will use real data from Twitter from 2010. The dataset represents the follower graph that 
contains links between tweeting users in the form: user_id, follower_id (where user_id is the id of 
a user and follower_id
   is    the    id    of    the    follower).    For    example:                         
12,13
12,14

12,15
16,17
Here, users 13, 14 and 15 are followers of user 12, while user 17 is a follower of user 16. The 
complete dataset is available on Expanse (file /expanse/lustre/projects/uot187/fegaras/large- 
twitter.csv) and contains 736,930 users and 36,743,448 links. A subset of this file (which contains 
the last 10,000 lines of the complete dataset) is available in small-twitter.csv inside project1.
First, for each twitter user, you count the number of users she follows. Then, you group the users 
by their number of the users they follow and for each group you count how many users belong to this 
group. That is, the result will have lines such as:
10 30



For matrix mul the following are the constraints
You should modify Multiply.java only. In your Java
main program, args[0] is the first input matrix M, args[1] is the second input matrix N, args[2] is the
directory name to pass the intermediate results from the first Map-Reduce job to the second, and args[3]
is the output directory. The file formats for the input and output must be text and the file format of the
intermediate results must be sequence file format (binary). There are two small sparse matrices 4*3 and
3*3 in the files M-matrix-small.txt and N-matrix-small.txt for testing in local mode. Their matrix multiplication
must return the 4*3 matrix in solution-small.txt. Then, there are two moderate-sized matrices
1000*500 and 500*1000 in the files M-matrix-large.txt and M-matrix-large.txt for testing in
distributed mode. The first and last lines of the matrix multiplication of these two matrices must must be
similar to those in solution-large.txt.




A directed graph is represented in the input text file using one line per graph vertex. For example, the line
1,2,3,4,5,6,7
represents the vertex with ID 1, which is linked to the vertices with IDs 2, 3, 4, 5, 6, and 7. Your task is to
write a Map-Reduce program that partitions a graph into K clusters using multi-source BFS (breadth-first
search). It selects K random graph vertices, called centroids, and then, at the first iteration, for each
centroid, it assigns the centroid id to its unassigned neighbors. Then, at the second iteration. it assigns the
centroid id to the unassigned neighbors of the neighbors, etc, in a breadth-first search fashion. After few
repetitions, each vertex will be assigned to the centroid that needs the smallest number of hops to reach
the vertex (the closest centroid). First you need a class to represent a vertex:
class Vertex {
long id; // the vertex ID
Vector adjacent; // the vertex neighbors
long centroid; // the id of the centroid in which this vertex belongs to
short depth; // the BFS depth
...
}
Vertex has a constructor Vertex( id, adjacent, centroid, depth ).
You need to write 3 Map-Reduce tasks. The first Map-Reduce job is to read the graph:
map ( key, line ) =
parse the line to get the vertex id and the adjacent vector
// take the first 10 vertices of each split to be the centroids
for the first 10 vertices, centroid = id; for all the others, centroid = -1
emit( id, new Vertex(id,adjacent,centroid,0) )
The second Map-Reduce job is to do BFS:
map ( key, vertex ) =
emit( vertex.id, vertex ) // pass the graph topology
if (vertex.centroid > 0)
for n in vertex.adjacent: // send the centroid to the adjacent vertices
emit( n, new Vertex(n,[],vertex.centroid,BFS_depth) )
reduce ( id, values ) =
min_depth = 1000
m = new Vertex(id,[],-1,0)
for v in values:
if (v.adjacent is not empty)
m.adjacent = v.adjacent
if (v.centroid > 0 && v.depth < min_depth)
min_depth = v.depth
m.centroid = v.centroid
m.depth = min_depth
emit( id, m )
The final Map-Reduce job is to calculate the cluster sizes:
map ( id, value ) =
emit(value.centroid,1)
reduce ( centroid, values ) =
m = 0
for v in values:
m = m+v
emit(centroid,m)
The second map-reduce job must be repeated multiple times. For your project, repeat it 8 times. The
variable BFS_depth is bound to the iteration number (from 1 to 8). The args vector in your main program
has the following path names: args[0] is the input graph, args[1] is the intermediate directory (tmp), and
args[2] is the output. The first Map-Reduce job writes on the directory args[1]+"/i0". The second Map-
Reduce job reads from the directory args[1]+"/i"+i and writes in the directory args[1]+"/i"+(i+1),
where i is the for-loop index you use to repeat the second Map-Reduce job. The final Map-Reduce job
reads from args[1]+"/i8" and writes on args[2]. Note that the intermediate results between Map-
Reduce jobs must be stored using SequenceFileOutputFormat.
A skeleton file project3/src/main/java/GraphPartition.java is provided, as well as scripts to
build and run this code on Expanse. You should modify GraphPartition.java only. There is one small
graph in small-graph.txt for testing in standalone mode. Then, there is a moderate-sized graph
large-graph.txt for testing in distributed mode.











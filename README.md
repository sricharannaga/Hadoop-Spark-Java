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








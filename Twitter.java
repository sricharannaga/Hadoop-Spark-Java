import java.io.*;
import java.util.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;


public class Twitter {

    public static void main ( String[] args ) throws Exception {
        Job job1 = Job.getInstance();
        job1.setJarByClass(Twitter.class);
        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(IntWritable.class);
        job1.setMapperClass(MyMapper1.class);
        job1.setReducerClass(MyReducer1.class);
        job1.setInputFormatClass(TextInputFormat.class);
        job1.setOutputFormatClass(TextOutputFormat.class);
        FileInputFormat.setInputPaths(job1, new Path(args[0]));
        FileOutputFormat.setOutputPath(job1, new Path(args[1]));
        boolean status = job1.waitForCompletion(true);

        Job job2 = Job.getInstance();
        job2.setJarByClass(Twitter.class);
        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(IntWritable.class);
        job2.setMapperClass(MyMapper2.class);
        job2.setReducerClass(MyReducer2.class);
        job2.setInputFormatClass(TextInputFormat.class);
        job2.setOutputFormatClass(TextOutputFormat.class);
        FileInputFormat.setInputPaths(job2, new Path(args[1]));
        FileOutputFormat.setOutputPath(job2, new Path(args[2]));
        boolean job2_status = job2.waitForCompletion(true);

    }


    public static class MyMapper1 extends
            Mapper<LongWritable, Text, Text, IntWritable> {
        private Text Key_In_First_Mapper = new Text();

        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            String First_line_In_Every_Row = value.toString();
            Scanner scanner = new Scanner(First_line_In_Every_Row).useDelimiter(",");
            String userId = scanner.next(); // Assumes user ID is first in line
            String followerIds = scanner.next();

            scanner = new Scanner(followerIds);
            while (scanner.hasNext()) {
                String followerId = scanner.next();
                context.write(new Text(followerId), new IntWritable(1));
            }
            scanner.close();


        }
    }
    public static class MyReducer1 extends
            Reducer<Text, IntWritable, Text, IntWritable> {
        public void reduce(Text key, Iterable<IntWritable> follower_count,
                           Context context) throws IOException, InterruptedException {

            Iterator<IntWritable> it = follower_count.iterator();
            int first_count = 0;
            while (it.hasNext()) {
                IntWritable followers = it.next();
                first_count += followers.get();
            }
            context.write(key, new IntWritable(first_count));
        }
    }
    public static class MyMapper2 extends
            Mapper<LongWritable, Text, Text, IntWritable> {


        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {

            String First_line_In_Every_Row_From_Reducer1 = value.toString();
            Scanner scanner2 = new Scanner(First_line_In_Every_Row_From_Reducer1).useDelimiter("\t");
            String userId_1 = scanner2.next(); // Assumes user ID is first in line
            String followerIds_1 = scanner2.next();

            scanner2 = new Scanner(followerIds_1);
            while (scanner2.hasNext()) {
                String followerId_2 = scanner2.next();
                context.write(new Text(followerId_2), new IntWritable(1));
            }
            scanner2.close();



        }
    }

    public static class MyReducer2 extends
            Reducer<Text, IntWritable, Text, IntWritable> {
        public void reduce(Text key, Iterable<IntWritable> follower_count2,
                           Context context) throws IOException, InterruptedException {
            Iterator<IntWritable> it1 = follower_count2.iterator();
            int final_count = 0;
            while (it1.hasNext()) {
                IntWritable followers = it1.next();
                final_count += followers.get();
            }
            context.write(key, new IntWritable(final_count));
        }
    }
}
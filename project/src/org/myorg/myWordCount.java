package org.myorg;
import java.io.IOException;
import java.util.*;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.util.*;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapred.JobConf;
public class myWordCount {
    public static class Map extends Mapper
        <LongWritable, Text, Text, IntWritable> {
            private final static IntWritable one = new IntWritable(1);
            private Text word = new Text();

            public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
                String line = value.toString();
                StringTokenizer tokenizer = new StringTokenizer(line);
                while (tokenizer.hasMoreTokens()) {
                    word.set(tokenizer.nextToken());
                    context.write(word, one);
                }
            }
        }
    public static class Reduce extends Reducer
        <Text, IntWritable, Text, IntWritable> {
            public void reduce(Text key, Iterator<IntWritable> values, Context context) throws IOException, InterruptedException {
                int sum = 0;
                while (values.hasNext()) {
                    sum += values.next().get();
                }
                context.write(key, new IntWritable(sum));
            }
        }
        public static void main(String[] args) throws Exception {
            Configuration conf = new Configuration();
            conf.set("mapreduce.job.queuename", "apg_p7");
            System.out.println("This is a new version");
            Job job = new Job(conf);
            job.setJarByClass(myWordCount.class);
            job.setJobName("myWordCount");
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(IntWritable.class);
            job.setMapperClass(myWordCount.Map.class);
            job.setCombinerClass(myWordCount.Reduce.class);
            job.setReducerClass(myWordCount.Reduce.class);
            FileInputFormat.addInputPath(job, new Path(args[0]));
            FileOutputFormat.setOutputPath(job, new Path(args[1]));
            job.waitForCompletion(true);
        }
}


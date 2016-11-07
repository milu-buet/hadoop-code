package org.myorg;
import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.math.BigInteger;

public class myWordCount2 {

  public static class TokenizerMapper
       extends Mapper<Object, Text, Text, IntWritable>{

    private final static IntWritable one = new IntWritable(1);
    private Text word = new Text();

    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {

      StringTokenizer itr = new StringTokenizer(value.toString());
      while (itr.hasMoreTokens()) {
        String num = itr.nextToken();

        if(isPrimeBIG(num)){
            word.set(num);
            context.write(word, one);

        }
        else{
            String nprime = nextPrime(num);
            word.set(nprime);
            context.write(word, one);


        }

      }
    }

    public boolean isPrime(String num){

        int n = Integer.parseInt(num);

        if(n<2) return false;
        else if(n==2 || n==3) return true;
        else if(n%2 == 0 || n%3 == 0) return false;
        for(int i=5;i<n/2;i++){
            if(n%i==0) return false;
            i++;
        }
        return true;
    }

    public String nextPrime(String num){

        BigInteger n = new BigInteger(num);
        BigInteger nprime = n.nextProbablePrime();

        // while(!isPrimeBIG(nprime.toString())){
        //     nprime = nprime.nextProbablePrime();
        // }
        return nprime.toString();
    }


    public boolean isPrimeBIG(String num){

        BigInteger n = new BigInteger(num);
        BigInteger zero = new BigInteger("0");
        BigInteger two = new BigInteger("2");
        BigInteger three = new BigInteger("3");
        BigInteger five = new BigInteger("5");

        if(n.isProbablePrime(10) == false) return false;

        if(n.compareTo(two) < 0) return false;
        else if(n.equals(two) || n.equals(three)) return true;
        else if(n.remainder(two).equals(zero) || n.remainder(three).equals(zero)) {
            return false;
        }

        BigInteger n_root = sqrt(n);
        //System.out.println(">>>>");
        //System.out.println(n_root);

        for(BigInteger i = five ;n_root.compareTo(i) > 0;){
            if(n.remainder(i) == BigInteger.ZERO) return false;
            i = i.add(two);
            //System.out.println(i);
        }
        return true;
    }


    public BigInteger sqrt(BigInteger n) {
        BigInteger a = BigInteger.ONE;
        BigInteger b = n.shiftRight(5).add(BigInteger.valueOf(8));
        while (b.compareTo(a) >= 0) {
            BigInteger mid = a.add(b).shiftRight(1);
            if (mid.multiply(mid).compareTo(n) > 0) {
                b = mid.subtract(BigInteger.ONE);
            } else {
                a = mid.add(BigInteger.ONE);
            }
        }
        return a.subtract(BigInteger.ONE);
    }
    
  }

  public static class IntSumReducer
       extends Reducer<Text,IntWritable,Text,IntWritable> {
    private IntWritable result = new IntWritable();

    public void reduce(Text key, Iterable<IntWritable> values,
                       Context context
                       ) throws IOException, InterruptedException {
      int sum = 0;
      for (IntWritable val : values) {
        sum += val.get();
        System.out.println(val);
      }
      result.set(sum);
      context.write(key, result);
    }
  }

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "word count");
    job.setJarByClass(myWordCount2.class);
    job.setMapperClass(TokenizerMapper.class);
    job.setCombinerClass(IntSumReducer.class);
    job.setReducerClass(IntSumReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}
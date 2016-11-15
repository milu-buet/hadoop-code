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
       extends Mapper<Object, Text, Text, Text>{

    //private final static IntWritable one = new IntWritable(1);
    private Text word = new Text();
    private Text one_ = new Text("prime");
    private Text zero_ = new Text("not-prime");
    private String unknown_ = "-1";
    private BigInteger two = new BigInteger("2");

    private BigInteger chank_no = new BigInteger("5");


    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {

      StringTokenizer itr = new StringTokenizer(value.toString());
      while (itr.hasMoreTokens()) {
        String num = itr.nextToken();
        word.set(num);

        int incheck = isPrimeInitial(num);
        if(incheck == 0){
            context.write(word, zero_);
        }
        else if(incheck == 1){
            context.write(word, one_); 
        }
        else{
            BigInteger n = new BigInteger(num);
            BigInteger n_root = sqrt(n);
            BigInteger chank_size = n_root.divide(chank_no);
            BigInteger ichank_size = chank_size;

            if(ichank_size.remainder(two).equals(BigInteger.ONE)){
                ichank_size = ichank_size.add(BigInteger.ONE);
            }

            if(ichank_size.compareTo(two) < 0){
                ichank_size = two;
            }


            BigInteger istart = new BigInteger("5");
            BigInteger iend = istart.add(ichank_size);


            for(BigInteger i = istart; n_root.compareTo(iend) >= 0 ; ){



                String mapped_problem = istart.toString() + ' ' + iend.toString() + ' ' + unknown_ ;
                
                //System.out.println(mapped_problem);

                Text mapped_problem_text = new Text(mapped_problem);
                context.write(word,mapped_problem_text);

                istart = istart.add(ichank_size).add(two);
                iend = iend.add(ichank_size).add(two);

            }

            iend = n_root;

            if(iend.remainder(two).equals(BigInteger.ZERO)){
                iend = iend.subtract(BigInteger.ONE);
            }


            String mapped_problem = istart.toString() + ' ' + iend.toString() + ' ' + unknown_ ;
            Text mapped_problem_text = new Text(mapped_problem);
            context.write(word,mapped_problem_text);

        }

      }
    }

    public int isPrimeInitial(String num){

        BigInteger n = new BigInteger(num);
        BigInteger zero = new BigInteger("0");
        BigInteger two = new BigInteger("2");
        BigInteger three = new BigInteger("3");
        BigInteger five = new BigInteger("5");

        if(n.isProbablePrime(10) == false) return 0;

        if(n.compareTo(two) < 0) return 0;
        else if(n.equals(two) || n.equals(three)) return 1;
        else if(n.remainder(two).equals(zero) || n.remainder(three).equals(zero)) {
            return 0;
        }
        return -1;

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
       extends Reducer<Text,Text,Text,Text> {
    private Text result = new Text();

    public void reduce(Text key, Iterable<Text> values,
                       Context context
                       ) throws IOException, InterruptedException {
      String r = "prime";
      for (Text val : values) {
            //sum += val.get();
            System.out.println(key);
            System.out.println(val);

            String num = key.toString();
            String[] params = val.toString().split(" ");

            if (params.length == 3){
                    boolean isprime =  isPrimeBIG(num,params[0],params[1]);
                    if(!isprime){
                        r = "not prime";
                        break;
                    }
            }
            else{

                    r = params[0];

            }

      }

      result.set(r);
      context.write(key, result);
    }


    public boolean isPrimeBIG(String num, String start_num, String end_num){

        BigInteger n = new BigInteger(num);
        BigInteger two = new BigInteger("2");
        BigInteger start = new BigInteger(start_num);
        BigInteger end = new BigInteger(end_num);

        if(start.remainder(two).equals(BigInteger.ZERO)){
            start.add(BigInteger.ONE);
        }

        for(BigInteger i = start ; end.compareTo(i) > 0;){
            if(n.remainder(i).equals(BigInteger.ZERO) ){
                return false;
            }
            i = i.add(two);
        }

        return true;
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
    job.setOutputValueClass(Text.class);
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}
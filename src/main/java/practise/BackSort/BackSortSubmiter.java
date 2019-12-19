package practise.BackSort;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Iterator;
import java.util.Properties;

public class BackSortSubmiter {

    public static class BackSortMapper extends Mapper<LongWritable, Text,Text,IntWritable>{
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String s = value.toString();
            String[] words = s.split(" ");
            FileSplit inputSplit = (FileSplit)context.getInputSplit();
            String[] paths = inputSplit.getPath().toString().split("/");
            String path = paths[paths.length-1];
            for (String word :
                    words) {
                context.write(new Text(word + "-" + path),new IntWritable(1));
            }
        }
    }

    public static class BackSortReducer extends Reducer<Text,IntWritable,Text,IntWritable>{
        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            Iterator<IntWritable> iterator = values.iterator();
            int sum = 0;
            while(iterator.hasNext()){
                IntWritable next = iterator.next();
                sum += next.get();
            }
            context.write(key,new IntWritable(sum));
        }
    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException, URISyntaxException {
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS","hdfs://hdp-01:9000");
        conf.set("mapreduce.framework.name","yarn");
        conf.set("yarn.resourcemanager.hostname","hdp-01");
        conf.set("mapreduce.app-submission.cross-platform","true");


        Properties properties = System.getProperties();
        properties.setProperty("HADOOP_USER_NAME","root");

        Job job = Job.getInstance(conf);
        job.setJar("D://BackSort.jar");

        job.setMapperClass(BackSortMapper.class);
        job.setReducerClass(BackSortReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        job.setNumReduceTasks(1);

        FileSystem fs = FileSystem.get(new URI("hdfs://hdp-01:9000"), conf, "root");
        if(fs.exists(new Path("/BackSort/output"))){
            fs.delete(new Path("/BackSort/output"),true);
        }

        FileInputFormat.setInputPaths(job,new Path("/BackSort/"));
        FileOutputFormat.setOutputPath(job,new Path("/BackSort/output/"));

        boolean b = job.waitForCompletion(true);

        System.exit(b?0:-1);
    }

}

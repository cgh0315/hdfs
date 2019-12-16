package practise.topn;

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
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Iterator;
import java.util.Properties;
import java.util.TreeMap;

public class TopNSubmiter {

    TreeMap<TopNBean,Object> treeMap = new TreeMap<>();

    public static class TopNMapper extends Mapper<LongWritable, Text, Text, IntWritable>{
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String s = value.toString();
            String[] s1 = s.split(" ");
            context.write(new Text(s1[0]),new IntWritable(1));
        }
    }

    public static class TopNReducer extends Reducer<Text,IntWritable,Text,IntWritable>{
        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            Iterator<IntWritable> iterator = values.iterator();
            int sum = 0;
            while (iterator.hasNext()){
                IntWritable next = iterator.next();
                sum += next.get();
            }

            context.write(key,new IntWritable(sum));
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {

        }
    }

    public static void main(String[] args) throws IOException, URISyntaxException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS","hdfs://hdp-01:/9000");
        conf.set("mapreduce.framework.name","yarn");
        conf.set("yarn.resourcemanager.hostname","hdp-01");
        conf.set("mapreduce.app-submission.cross-platform","true");

        Properties properties = System.getProperties();
        properties.setProperty("HADOOP_USER_NAME","root");

        Job job = Job.getInstance(conf);
        job.setJar("D://TopN.jar");

        job.setMapperClass(TopNMapper.class);
        job.setReducerClass(TopNReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputKeyClass(IntWritable.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        FileSystem root = FileSystem.get(new URI("hdfs://hdp-01:9000"), conf, "root");
        boolean exists = root.exists(new Path("/TopN/output/"));
        if(exists){
            root.delete(new Path("/TopN/output/"),true);
        }
        FileInputFormat.setInputPaths(job,new Path("/TopN/input/request.dat"));
        FileOutputFormat.setOutputPath(job,new Path("/TopN/output"));
        boolean b = job.waitForCompletion(true);
        System.exit(b?0:-1);

    }
}

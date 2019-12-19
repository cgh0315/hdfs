package practise.BackSort;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
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

public class BackSortSubmiter2 {

    public static class BackSortMapper2 extends Mapper<LongWritable, Text,Text,Text>{
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] split = value.toString().split("-");
            String[] value2 = split[1].split("\t");
            context.write(new Text(split[0]),new Text(value2[0]+"-->"+value2[1]));
            }
    }

    public static class BackSortReducer2 extends Reducer<Text,Text,Text,Text>{
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            Iterator<Text> iterator = values.iterator();
            String result = "";
            while (iterator.hasNext()){
                Text next = iterator.next();
                result = next.toString() + " " + result;
            }
            context.write(key,new Text(result));
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

        job.setMapperClass(BackSortMapper2.class);
        job.setReducerClass(BackSortReducer2.class);

        job.setMapOutputValueClass(Text.class);
        job.setMapOutputKeyClass(Text.class);

        job.setOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setNumReduceTasks(1);

        FileSystem root = FileSystem.get(new URI("hdfs://hdp-01:9000"), conf, "root");
        if(root.exists(new Path("/BackSort/output2/"))){
            root.delete(new Path("/BackSort/output2/"),true);
        }

        FileInputFormat.setInputPaths(job,new Path("/BackSort/output/"));
        FileOutputFormat.setOutputPath(job,new Path("/BackSort/output2/"));
        boolean b = job.waitForCompletion(true);
        System.exit(b?0:-1);
    }
}



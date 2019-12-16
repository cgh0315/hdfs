package practise.urlCount;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Properties;

/**
 * 将统计好的文件放在一个文件里
 */
public class JobSubmiter2 {

    public static class UrlMapper2 extends Mapper<LongWritable, Text,UrlBean, NullWritable>{
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String str = value.toString();
            String[] words = str.split("\t");
            String url = words[0];
            int mount = Integer.parseInt(words[1]);
            context.write(new UrlBean(url,mount),NullWritable.get());
        }
    }

    public static class UrlReducer2 extends Reducer<UrlBean,NullWritable,UrlBean,NullWritable>{
        @Override
        protected void reduce(UrlBean key, Iterable<NullWritable> values, Reducer<UrlBean, NullWritable, UrlBean, NullWritable>.Context context) throws IOException, InterruptedException {
            context.write(key,NullWritable.get());
        }
    }


    public static void main(String[] args) throws IOException, URISyntaxException, InterruptedException, ClassNotFoundException {
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS","hdfs://hdp-01:9000");
        conf.set("mapreduce.framework.name","yarn");
        conf.set("yarn.resourcemanager.hostname","hdp-01");
        conf.set("mapreduce.app-submission.cross-platform","true");

        Properties properties = System.getProperties();
        properties.setProperty("HADOOP_USER_NAME", "root");

        Job job = Job.getInstance(conf);
        job.setJar("D://Url.jar");

        job.setMapperClass(UrlMapper2.class);
        job.setReducerClass(UrlReducer2.class);

        job.setMapOutputKeyClass(UrlBean.class);
        job.setMapOutputValueClass(NullWritable.class);

        job.setOutputKeyClass(UrlBean.class);
        job.setOutputValueClass(NullWritable.class);

        job.setNumReduceTasks(1);

        FileSystem root = FileSystem.get(new URI("hdfs://hdp-01:9000"), conf, "root");
        boolean exists = root.exists(new Path("/"));
        if (exists){
            root.delete(new Path("/Url/output2"),true);
        }
        FileInputFormat.setInputPaths(job,new Path("/Url/output"));
        FileOutputFormat.setOutputPath(job,new Path("/Url/output2"));
        boolean b = job.waitForCompletion(true);
        System.exit(b? 0 : -1);

    }
}

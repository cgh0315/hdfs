package practise.urlCount;

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
import java.util.Properties;

/**
 * 求出每一个url被访问的总次数，并将结果输出到一个结果文件中
 * 数据：
 * 2017/07/28 qq.com/a
 * 2017/07/28 qq.com/bx
 * 2017/07/28 qq.com/by
 * 思路：
 *     第一个job：先将数据统计出来成为 key,1,key,3
 *     第二个job：将第一步的结果，通过在reduce的过程中以<对象，NullWritable>的形式输出，并且reduceTask只能有一个
 * 输出在一个文件的关键：
 *     在第二个job的reduce过程中只能有一个reduceTask
 */
public class JobSubmiter {

    public static class UrlMapper extends Mapper<LongWritable, Text,Text, IntWritable> {
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String str = value.toString();
            String[] url = str.split(" ");
            context.write(new Text(url[1]),new IntWritable(1));
        }
    }

    public static class UrlReducer extends Reducer<Text, IntWritable,Text,IntWritable> {
        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable i :
                    values) {
                sum+=i.get();
            }
            context.write(key,new IntWritable(sum));
        }
    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException, URISyntaxException {
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS","hdfs://hdp-01:9000");
        conf.set("mapreduce.framework.name", "yarn");
        conf.set("yarn.resourcemanager.hostname", "hdp-01");
        conf.set("mapreduce.app-submission.cross-platform","true");

        Properties properties = System.getProperties();
        properties.setProperty("HADOOP_USER_NAME", "root");

        Job job = Job.getInstance(conf);
        job.setJar("D://Url.jar");
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setMapperClass(UrlMapper.class);
        job.setReducerClass(UrlReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        job.setNumReduceTasks(3);

        FileSystem fs = FileSystem.get(new URI("hdfs://hdp-01:9000"), conf, "root");
        boolean exists = fs.exists(new Path("/Url/output"));
        if (exists){
            fs.delete(new Path("/Url/output"),true);
        }
        FileInputFormat.setInputPaths(job,new Path("/Url/input/request.dat"));
        FileOutputFormat.setOutputPath(job,new Path("/Url/output"));
        boolean b = job.waitForCompletion(true);
        System.exit(b?0:-1);

    }
}





















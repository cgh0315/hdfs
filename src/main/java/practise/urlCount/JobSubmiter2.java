package practise.urlCount;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Properties;

/**
 * 将统计好的文件放在一个文件里
 */
public class JobSubmiter2 {

    public static class UrlMapper2 extends Mapper<LongWritable, Text,UrlBean, NullWritable>{
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String str = value.toString();
            String[] words = str.split(" ");
            String url = words[0];
            int mount = Integer.parseInt(words[1]);
            context.write(new UrlBean(url,mount),NullWritable.get());
        }
    }

    public static class UrlReducer2 extends Reducer<UrlBean,NullWritable,UrlBean,NullWritable>{
        @Override
        protected void reduce(UrlBean key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {
            context.write(key,NullWritable.get());
        }
    }

    public static void main(String[] args) throws IOException {
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS","hdfs://hdp-01:9000");
        conf.set("mapreduce.framework.name","yarn");
        conf.set("yarn.resourcemanager.hostname","hdp-01");
        conf.set("mapreduce.app-submission.cross-platform","true");

        Properties properties = new Properties();
        properties.setProperty("HADOOP_USER_NAME","root");

        Job job = Job.getInstance(conf);
        job.setMapperClass(UrlMapper2.class);
        job.setReducerClass(UrlReducer2.class);
        job.setMapOutputKeyClass(UrlBean.class);
//        job.set
//        job.setOutputKeyClass();
    }
}

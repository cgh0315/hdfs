package mapreduce.pageCount;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Properties;

public class JobSubmitter2 {

    public static void main(String[] args) throws InterruptedException, IOException, ClassNotFoundException, URISyntaxException {
        // 1.config
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS","hdfs://hdp-01:9000");
        conf.set("mapreduce.framework.name", "yarn");
        conf.set("yarn.resourcemanager.hostname", "hdp-01");
        conf.set("mapreduce.app-submission.cross-platform","true");

        Properties properties = System.getProperties();
        properties.setProperty("HADOOP_USER_NAME", "root");

        // 2.job
        Job job = Job.getInstance(conf);
        job.setJar("D://wc.jar");
        job.setMapperClass(wordCountMapper.class);
        job.setReducerClass(wordCountReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        job.setNumReduceTasks(4);

        // 3.set value
        FileSystem fs = FileSystem.get(new URI("hdfs://hdp-01:9000"), conf, "root");
        boolean exists = fs.exists(new Path("/practData/flow/output"));
        if (exists){
            fs.delete(new Path("/practData/flow/output"),true);
        }
        FileInputFormat.setInputPaths(job,new Path("/wordcount/input"));
        FileOutputFormat.setOutputPath(job,new Path("/wordcount/output"));

        boolean b = job.waitForCompletion(true);

        System.exit(b? 0: -1);

    }

}

package mapreduce.flowCount;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Properties;

/**
 * 统计流量总数
 * 并且需要分区partition
 */
public class JobSubmit {
    public static void main(String[] args) throws InterruptedException, IOException, ClassNotFoundException, URISyntaxException {
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS","hdfs://hdp-01:9000");
        conf.set("mapreduce.Framework.name","yarn");
        conf.set("yarn.resourcemanager.hostname","hdp-01");
        conf.set("mapreduce.app-submission.cross-platform","true");

        Properties properties = new Properties();
        properties.setProperty("HADOOP_USER_NAME","root");

        Job job = Job.getInstance(conf);
        job.setJar("D:\\flow.jar");
        job.setMapperClass(FlowMapper.class);
        job.setReducerClass(FlowReducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(FlowBean.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(FlowBean.class);
        job.setNumReduceTasks(6);
        job.setPartitionerClass(ProvincePartitioner.class);

        FileSystem fs = FileSystem.get(new URI("hdfs://hdp-01:9000"), conf, "root");
        boolean exists = fs.exists(new Path("/practData/flow/output"));
        if (exists){
            fs.delete(new Path("/practData/flow/output"),true);
        }
        FileInputFormat.setInputPaths(job,new Path("/practData/flow.log"));
        FileOutputFormat.setOutputPath(job,new Path("/practData/flow/output"));

        boolean b = job.waitForCompletion(true);
        System.exit(b?0:-1);

    }
}

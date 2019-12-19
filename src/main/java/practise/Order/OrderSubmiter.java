package practise.Order;

import org.apache.commons.lang.ArrayUtils;
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
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

public class OrderSubmiter {

    public static class OrderMapper extends Mapper<LongWritable, Text,OrderBean, NullWritable>{
        OrderBean orderBean = new OrderBean();
        NullWritable v = NullWritable.get();
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] fields = value.toString().split(",");
            orderBean.set(fields[0], fields[1], fields[2], Float.parseFloat(fields[3]), Integer.parseInt(fields[4]));
            context.write(orderBean,v);
        }
    }

    public static class OrderReducer extends Reducer<OrderBean,NullWritable,OrderBean,NullWritable>{
        @Override
        protected void reduce(OrderBean key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {
            int i=0;
            for (NullWritable v : values) {
                context.write(key, v);
                if(++i==3) return;
            }
        }
    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException, URISyntaxException {
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS","hdfs://hdp-01:9000");
        conf.set("mapreduce.framework.name","yarn");
        conf.set("yarn.resourcemanager.hostname","hdp-01");
        conf.set("mapreduce.app-submission.cross-platform","true");
        conf.setInt("order.top.n", 2);

        Properties properties = System.getProperties();
        properties.setProperty("HADOOP_USER_NAME","root");

        Job job = Job.getInstance(conf);
        job.setJar("D://Order.jar");
        job.setNumReduceTasks(0);

        job.setMapperClass(OrderMapper.class);
        job.setReducerClass(OrderReducer.class);

        job.setMapOutputKeyClass(OrderBean.class);
        job.setMapOutputValueClass(NullWritable.class);

        job.setOutputKeyClass(OrderBean.class);
        job.setOutputValueClass(NullWritable.class);

        job.setPartitionerClass(OrderPartitioner.class);
        job.setGroupingComparatorClass(OrderIdGroupingComparator.class);

        FileSystem root = FileSystem.get(new URI("hdfs://hdp-01:9000"), conf, "root");
        boolean exists = root.exists(new Path("/Order/output/"));
        if(exists){
            root.delete(new Path("/Order/output/"),true);
        }
        FileInputFormat.setInputPaths(job,new Path("/Order/input"));
        FileOutputFormat.setOutputPath(job,new Path("/Order/output"));
        boolean b = job.waitForCompletion(true);
        System.exit(b?0:-1);

    }
}

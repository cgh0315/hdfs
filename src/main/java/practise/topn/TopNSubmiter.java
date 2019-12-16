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
import java.util.*;

public class TopNSubmiter {

    public static class TopNMapper extends Mapper<LongWritable, Text, Text, IntWritable>{

        @Override
        protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, IntWritable>.Context context)
                throws IOException, InterruptedException {
            String line = value.toString();
            String[] split = line.split(" ");
            context.write(new Text(split[1]), new IntWritable(1));
        }

    }

    public static class TopNReducer extends Reducer<Text, IntWritable, Text, IntWritable>{

        TreeMap<TopNBean, Object> treeMap = new TreeMap<>();

        @Override
        protected void reduce(Text key, Iterable<IntWritable> values,
                              Reducer<Text, IntWritable, Text, IntWritable>.Context context) throws IOException, InterruptedException {
            int count = 0;
            for (IntWritable value : values) {
                count += value.get();
            }
            TopNBean pageCount = new TopNBean(key.toString(), count);
            treeMap.put(pageCount,null);

        }


        @Override
        protected void cleanup(Context context)
                throws IOException, InterruptedException {
            Configuration conf = context.getConfiguration();
            int topn = conf.getInt("topN", 5);


            Set<Map.Entry<TopNBean, Object>> entrySet = treeMap.entrySet();
            int i= 0;

            for (Map.Entry<TopNBean, Object> entry : entrySet) {
                context.write(new Text(entry.getKey().getUrl()), new IntWritable(entry.getKey().getMount()));
                i++;
                if(i==topn) return;
            }


        }


    }

    public static void main(String[] args) throws IOException, URISyntaxException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS","hdfs://hdp-01:9000");
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
        job.setMapOutputValueClass(IntWritable.class);

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

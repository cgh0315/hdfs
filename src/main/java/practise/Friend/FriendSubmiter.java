package practise.Friend;

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
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

/**
 * 数据：
 *  A: B, C, D , E
 *  B: C, F
 *  C: A, B
 *
 *  思路：
 *      将右边的作为key，左边的为value
 *      在reduce阶段，收集到有B为好友的人，然后排列组合（例如：B :   A,C,D,E)
 *      则将A-C B  表示AC的共同好友为B
 */
public class FriendSubmiter {

    public static class FriendMap extends Mapper<LongWritable, Text,Text,Text>{
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] split = value.toString().split(":");
            String[] friends = split[1].split(",");
            for (String fri :
                    friends) {
                context.write(new Text(fri), new Text(split[0]));
            }
        }
    }

    public static class FriendReducer extends Reducer<Text,Text,Text,Text>{
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            List<String> list = new ArrayList<>();
            values.forEach(a->list.add(a.toString()));
            for(int i = 0; i < list.size()-1; i ++){
                for(int j = i+1; j < list.size()-1; j++){
                    context.write(new Text(list.get(i)+"-"+list.get(j).toString()),new Text(key));
                }
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
        properties.setProperty("HADOOP_USER_NAME", "root");

        Job job = Job.getInstance(conf);
        job.setNumReduceTasks(1);
        job.setJar("D://Friend.jar");

        job.setMapperClass(FriendMap.class);
        job.setReducerClass(FriendReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.setInputPaths(job,new Path("/Friend/input"));
        FileOutputFormat.setOutputPath(job,new Path("/Friend/output"));

        FileSystem root = FileSystem.get(new URI("hdfs://hdp-01:9000"), conf, "root");
        if(root.exists(new Path("/Friend/output"))){
            root.delete(new Path("/Friend/output"),true);
        }

        boolean b = job.waitForCompletion(true);
        System.exit(b?0:-1);
    }

}

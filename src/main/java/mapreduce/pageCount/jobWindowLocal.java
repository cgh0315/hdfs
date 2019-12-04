package mapreduce.pageCount;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * 统计
 */
public class jobWindowLocal {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();

        //conf.set("fs.defaultFS", "file:///");
        //conf.set("mapreduce.framework.name", "local");

        Job job = Job.getInstance(conf);

        job.setJarByClass(jobWindowLocal.class);

        job.setMapperClass(wordCountMapper.class);
        job.setReducerClass(wordCountReducer.class);

        job.setMapOutputKeyClass(Text .class);
        job.setMapOutputValueClass(IntWritable .class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        FileInputFormat.setInputPaths(job, new Path("D:/input"));
        FileOutputFormat.setOutputPath(job, new Path("D:/output"));

        job.setNumReduceTasks(3);

        boolean res = job.waitForCompletion(true);
        System.exit(res?0:1);
    }
}

package mapreduce.pageCount;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class wordCountMapper extends Mapper<LongWritable,Text,Text, IntWritable> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String s = value.toString();
        String[] words = s.split(" ");
        for (String word:
             words) {
            context.write(new Text(word),new IntWritable(1));
        }
    }
}

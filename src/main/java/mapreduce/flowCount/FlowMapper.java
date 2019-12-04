package mapreduce.flowCount;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


import java.io.IOException;

public class FlowMapper extends Mapper<LongWritable,Text,Text,FlowBean >{
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String str = value.toString();
        String[] split = str.split("\t");
        String phone = split[1];
        int uFlow = Integer.parseInt(split[split.length - 3]);
        int dFlow = Integer.parseInt(split[split.length - 2]);
        context.write(new Text(phone),new FlowBean(uFlow,dFlow,phone));
    }
}

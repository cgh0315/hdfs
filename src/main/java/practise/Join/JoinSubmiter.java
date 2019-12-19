package practise.Join;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;

public class JoinSubmiter {

    public static class JoinMapper extends Mapper<LongWritable, Text,Text,JoinBean>{
        String fileName = null;
        JoinBean joinBean = new JoinBean();
        Text t = new Text();

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            FileSplit inputSplit = (FileSplit)context.getInputSplit();
            String[] split = inputSplit.getPath().getName().split("/");
            fileName = split[split.length - 1];
        }

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] fields = value.toString().split(",");
            if (fileName.startsWith("order")) {
                joinBean.set(fields[0],fields[1], "NULL", -1, "NULL", "order" );
            }else {
                joinBean.set("NULL", fields[0], fields[1], Integer.parseInt(fields[2]), fields[3], "user");
            }
            t.set(joinBean.getUserId());
            context.write(t,joinBean);
        }
    }

    public static class JoinReducer extends Reducer<Text,JoinBean,JoinBean, NullWritable>{
        @Override
        protected void reduce(Text key, Iterable<JoinBean> values, Context context) throws IOException, InterruptedException {
            Iterator<JoinBean> iterator = values.iterator();
            List<JoinBean> orderList = new ArrayList<>();
            JoinBean orderBe = null;
            JoinBean userBean = null;
            while (iterator.hasNext()){
                orderBe = iterator.next();
                if("order".equals(orderBe.getTableName())){
                    orderList.add(orderBe);
                }else {
                    userBean = orderBe;
                }
            }

            for (JoinBean bean :
                    orderList) {
                bean.setUserName(userBean.getUserName());
                bean.setUserAge(userBean.getUserAge());
                bean.setUserFriend(userBean.getUserFriend());
                context.write(bean, NullWritable.get());
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
        job.setNumReduceTasks(1);
        job.setJar("D://Join.jar");

        job.setMapperClass(JoinMapper.class);
        job.setReducerClass(JoinReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(JoinBean.class);

        job.setOutputKeyClass(JoinBean.class);
        job.setOutputValueClass(NullWritable.class);



        FileSystem root = FileSystem.get(new URI("hdfs://hdp-01:9000"), conf, "root");
        if(root.exists(new Path("/Join/output"))){
            root.delete(new Path("/Join/output"),true);
        }

        FileInputFormat.setInputPaths(job,new Path("/Join/input"));
        FileOutputFormat.setOutputPath(job,new Path("/Join/output"));

        boolean b = job.waitForCompletion(true);

        System.exit(b?0:-1);


    }
}

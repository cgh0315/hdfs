package wordCount;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

public class HdfsWordcount {

    public static void main(String[] args) throws IllegalAccessException, InstantiationException, URISyntaxException, IOException, InterruptedException, ClassNotFoundException {
        Properties properties = new Properties();
        properties.load(HdfsWordcount.class.getClassLoader().getResourceAsStream("job.properties"));
        String input = properties.getProperty("input");
        String output = properties.getProperty("output");

        Class<?> mapper = Class.forName(properties.getProperty("mapper"));
        Mapper wordMapper = (Mapper)mapper.newInstance();
        Context context = new Context();

        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(new URI("hdfs://hdp-01:9000"), conf, "root");
        FSDataInputStream open = fs.open(new Path("/aa.txt"));
        BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(open, "utf-8"));
        String line = "";
        while((line = bufferedReader.readLine()) != null){
            wordMapper.mapper(line,context);
        }
        bufferedReader.close();
        open.close();

        FSDataOutputStream open1 = fs.create(new Path("/bb.txt"));
        Map map = context.getMap();

        Set<Map.Entry<Object,Object>> entrySet = map.entrySet();
        for (Map.Entry<Object, Object> entry : entrySet) {
            open1.write((entry.getKey().toString()+"\t"+entry.getValue()+"\n").getBytes());
        }

    }
}

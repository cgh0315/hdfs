package practise.topn;

import org.apache.hadoop.conf.Configuration;

public class TestConf {
    public static void main(String[] args) {
        Configuration conf = new Configuration();
        conf.addResource("xxoo.xml");
        System.out.println(conf.get("topn"));
    }
}

package mapreduce.flowCount;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

import java.util.HashMap;
import java.util.Map;

public class ProvincePartitioner extends Partitioner {
    private static Map<String,Integer> codeMap = new HashMap<>();

    static {
        codeMap.put("135", 0);
        codeMap.put("136", 1);
        codeMap.put("137", 2);
        codeMap.put("138", 3);
        codeMap.put("139", 4);
    }

    @Override
    public int getPartition(Object text, Object o2, int i) {
        Integer integer = codeMap.get(text.toString().substring(0, 3));
        return integer == null? 5:integer;
    }
}

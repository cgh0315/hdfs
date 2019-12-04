package mapreduce.flowCount;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class FlowReducer extends Reducer<Text,FlowBean,Text,FlowBean> {
    @Override
    protected void reduce(Text key, Iterable<FlowBean> values, Context context) throws IOException, InterruptedException {
        int uFlow = 0;
        int dFlow = 0;
        for (FlowBean flowBean : values) {
            uFlow += flowBean.getUpFlow();
            dFlow += flowBean.getdFlow();
        }
        context.write(key,new FlowBean(uFlow,dFlow,key.toString()));
    }
}

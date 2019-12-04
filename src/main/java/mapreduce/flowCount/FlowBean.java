package mapreduce.flowCount;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class FlowBean implements Writable {

    private int upFlow;
    private int dFlow;
    private String phone;
    private int amountFlow;

    public int getUpFlow() {
        return upFlow;
    }

    public void setUpFlow(int upFlow) {
        this.upFlow = upFlow;
    }

    public int getdFlow() {
        return dFlow;
    }

    public void setdFlow(int dFlow) {
        this.dFlow = dFlow;
    }

    public String getPhone() {
        return phone;
    }

    public void setPhone(String phone) {
        this.phone = phone;
    }

    public int getAmountFlow() {
        return amountFlow;
    }

    public void setAmountFlow(int amountFlow) {
        this.amountFlow = amountFlow;
    }

    public FlowBean() {
    }

    public FlowBean(int upFlow, int dFlow, String phone){
        this.phone = phone;
        this.upFlow = upFlow;
        this.dFlow = dFlow;
        this.amountFlow = upFlow + dFlow;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeInt(upFlow);
        dataOutput.writeInt(dFlow);
        dataOutput.writeInt(amountFlow);
        dataOutput.writeUTF(phone);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.upFlow = dataInput.readInt();
        this.dFlow = dataInput.readInt();
        this.amountFlow = dataInput.readInt();
        this.phone = dataInput.readUTF();
    }

    @Override
    public String toString() {
        return "FlowBean{" +
                "upFlow=" + upFlow +
                ", dFlow=" + dFlow +
                ", phone='" + phone + '\'' +
                ", amountFlow=" + amountFlow +
                '}';
    }
}

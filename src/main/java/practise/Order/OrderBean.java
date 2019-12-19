package practise.Order;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.Serializable;

public class OrderBean implements WritableComparable<OrderBean> , Serializable {
    private String orderId;
    private String userId;
    private String pdtName;
    private float price;
    private int number;
    private float amountFee;



    public String getOrderId() {
        return orderId;
    }

    public void setOrderId(String orderId) {
        this.orderId = orderId;
    }

    public String getUserId() {
        return userId;
    }

    public void setUserId(String userId) {
        this.userId = userId;
    }

    public String getPdtName() {
        return pdtName;
    }

    public void setPdtName(String pdtName) {
        this.pdtName = pdtName;
    }

    public float getPrice() {
        return price;
    }

    public void setPrice(float price) {
        this.price = price;
    }

    public int getNumber() {
        return number;
    }

    public void setNumber(int number) {
        this.number = number;
    }

    public float getAmountFee() {
        return amountFee;
    }

    public void setAmountFee(float amountFee) {
        this.amountFee = amountFee;
    }

    public void set(String orderId, String userId, String pdtName, float price, int number) {
        this.orderId = orderId;
        this.userId = userId;
        this.pdtName = pdtName;
        this.price = price;
        this.number = number;
        this.amountFee = price * number;
    }

    @Override
    public int compareTo(OrderBean o) {
        return this.orderId.compareTo(o.getOrderId())==0?Float.compare(o.getAmountFee(), this.getAmountFee()):this.orderId.compareTo(o.getOrderId());
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeUTF(this.orderId);
        dataOutput.writeUTF(this.userId);
        dataOutput.writeUTF(this.pdtName);
        dataOutput.writeFloat(this.price);
        dataOutput.writeInt(this.number);
        dataOutput.writeFloat(this.amountFee);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.orderId = dataInput.readUTF();
        this.userId = dataInput.readUTF();
        this.pdtName = dataInput.readUTF();
        this.price = dataInput.readFloat();
        this.number = dataInput.readInt();
        this.amountFee = this.price * this.number ;
    }

    @Override
    public String toString() {
        return "OrderBean{" +
                "orderId='" + orderId + '\'' +
                ", userId='" + userId + '\'' +
                ", pdtName='" + pdtName + '\'' +
                ", price=" + price +
                ", number=" + number +
                ", amountFee=" + amountFee +
                '}';
    }
}

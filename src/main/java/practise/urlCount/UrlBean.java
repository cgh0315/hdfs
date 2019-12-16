package practise.urlCount;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class UrlBean implements WritableComparable<UrlBean> {
    String url;
    int mount;

    public UrlBean(){

    }

    public UrlBean(String url, int mount) {
        this.url = url;
        this.mount = mount;
    }

    public String getUrl() {
        return url;
    }

    public void setUrl(String url) {
        this.url = url;
    }

    public int getMount() {
        return mount;
    }

    public void setMount(int mount) {
        this.mount = mount;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeUTF(url);
        dataOutput.writeInt(mount);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.url = dataInput.readUTF();
        this.mount = dataInput.readInt();
    }

    @Override
    public String toString() {
        return "UrlBean{" +
                "url='" + url + '\'' +
                ", mount=" + mount +
                '}';
    }

    @Override
    public int compareTo(UrlBean o) {
        return o.getMount()-this.mount==0?this.url.compareTo(o.getUrl()):o.getMount()-this.mount;
    }
}

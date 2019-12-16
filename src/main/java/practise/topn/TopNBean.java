package practise.topn;

public class TopNBean implements Comparable<TopNBean> {

    private String url;
    private int mount;

    public TopNBean() {
    }

    public TopNBean(String url, int mount) {
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
    public String toString() {
        return "TopNBean{" +
                "url='" + url + '\'' +
                ", mount=" + mount +
                '}';
    }

    @Override
    public int compareTo(TopNBean o) {
        return o.getMount() - this.mount == 0 ? this.url.compareTo(o.getUrl()):o.getMount() - this.mount;
    }
}

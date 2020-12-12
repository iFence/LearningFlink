package it.kenn.demo;

/**
 * 对接HBASE数据的pojo，也不是user，就，，这样叫吧
 */
public class HbaseUser {
    private Integer count;
    private String user;

    public HbaseUser(Integer count, String user) {
        this.count = count;
        this.user = user;
    }

    public HbaseUser(){}

    public Integer getCount() {
        return count;
    }

    public void setCount(Integer count) {
        this.count = count;
    }

    public String getUser() {
        return user;
    }

    public void setUser(String user) {
        this.user = user;
    }

    @Override
    public String toString() {
        return "HbaseUser{" +
                "count=" + count +
                ", user='" + user + '\'' +
                '}';
    }
}

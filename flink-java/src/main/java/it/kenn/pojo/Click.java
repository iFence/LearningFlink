package it.kenn.pojo;


public class Click{
    private String user;
    private String site;
    private String time;

    public String getUser() {
        return user;
    }

    public void setUser(String user) {
        this.user = user;
    }

    public String getSite() {
        return site;
    }

    public void setSite(String site) {
        this.site = site;
    }

    public String getTime() {
        return time;
    }

    public void setTime(String time) {
        this.time = time;
    }

    @Override
    public String toString() {
        return "Click{" +
                "user='" + user + '\'' +
                ", site='" + site + '\'' +
                ", time='" + time + '\'' +
                '}';
    }
}

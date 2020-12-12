package it.kenn.entities;

import java.util.List;

/**
 * @author yulei
 * @version 2020.11.20
 */
public class User {
    private Integer id;
    private String name;
    private Double salary;
    private List<String> friends;

    public User(Integer id, String name, Double salary, List<String> friends) {
        this.id = id;
        this.name = name;
        this.salary = salary;
        this.friends = friends;
    }

    public User() {}

    public Integer getId() {
        return id;
    }

    public void setId(Integer id) {
        this.id = id;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public Double getSalary() {
        return salary;
    }

    public void setSalary(Double salary) {
        this.salary = salary;
    }

    public List<String> getFriends() {
        return friends;
    }

    public void setFriends(List<String> friends) {
        this.friends = friends;
    }

    @Override
    public String toString() {
        return "User{" +
                "id=" + id +
                ", name='" + name + '\'' +
                ", salary=" + salary +
                ", friends=" + friends +
                '}';
    }
}

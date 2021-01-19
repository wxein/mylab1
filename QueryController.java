package com.example.demo.pojo;

//java bean
public class Tree {
    private int id;
    private int pId;
    private String name;
    private String url;

    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

    public int getpId() {
        return pId;
    }

    public void setpId(int pId) {
        this.pId = pId;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getUrl() {
        return url;
    }

    public void setUrl(String url) {
        this.url = url;
    }

    @Override
    public String toString() {
        return "Tree{" +
                "id=" + id +
                ", pid=" + pId +
                ", name='" + name + '\'' +
                ", url='" + url + '\'' +
                '}';
    }
}


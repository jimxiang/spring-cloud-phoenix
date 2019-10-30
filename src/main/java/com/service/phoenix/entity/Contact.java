package com.service.phoenix.entity;

import java.io.Serializable;

public class Contact implements Serializable {
    private static final long serialVersionUID = 1L;

    private String phone;
    private String name;
    private String createTime;
    private String uploadTime;
    private String available;
    private String source;
    private String phoneUid;
    private String registerTime;

    public Contact() {
        super();
    }

    public String getPhone() {
        return phone;
    }

    public void setPhone(String phone) {
        this.phone = phone;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getCreateTime() {
        return createTime;
    }

    public void setCreateTime(String createTime) {
        this.createTime = createTime;
    }

    public String getUploadTime() {
        return uploadTime;
    }

    public void setUploadTime(String uploadTime) {
        this.uploadTime = uploadTime;
    }

    public String getAvailable() {
        return available;
    }

    public void setAvailable(String available) {
        this.available = available;
    }

    public String getSource() {
        return source;
    }

    public void setSource(String source) {
        this.source = source;
    }

    public String getPhoneUid() {
        return phoneUid;
    }

    public void setPhoneUid(String phoneUid) {
        this.phoneUid = phoneUid;
    }

    public String getRegisterTime() {
        return registerTime;
    }

    public void setRegisterTime(String registerTime) {
        this.registerTime = registerTime;
    }

    @Override
    public String toString() {
        return "Contact{" +
                "phone='" + phone + '\'' +
                ", name='" + name + '\'' +
                ", createTime='" + createTime + '\'' +
                ", uploadTime='" + uploadTime + '\'' +
                ", available='" + available + '\'' +
                ", source='" + source + '\'' +
                ", phoneUid='" + phoneUid + '\'' +
                ", registerTime='" + registerTime + '\'' +
                '}';
    }
}

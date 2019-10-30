package com.service.phoenix.entity;

import java.util.List;

public class ContactList {
    private List<Contact> contacts;
    private int total;

    public ContactList() {
        super();
    }

    public List<Contact> getContacts() {
        return contacts;
    }

    public void setContacts(List<Contact> contacts) {
        this.contacts = contacts;
    }

    public int getTotal() {
        return total;
    }

    public void setTotal(int total) {
        this.total = total;
    }

    @Override
    public String toString() {
        return "ContactList{" +
                "contacts=" + contacts +
                ", total=" + total +
                '}';
    }
}

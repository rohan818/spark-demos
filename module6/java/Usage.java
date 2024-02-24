package main.java.ch6;

import java.io.Serializable;

// The Usage class represents a user's usage data, implementing Serializable to allow it to be used in distributed computing environments like Spark
public class Usage implements Serializable {
    // Properties of the Usage class
    int uid;                
    String uname;           
    int usage;              
    // Constructor to initialize Usage objects with specific values
    public Usage(int uid, String uname, int usage) {
        this.uid = uid;
        this.uname = uname;
        this.usage = usage;
    }

    // Getters and Setters
    public int getUid() { return this.uid; }
    public void setUid(int uid) { this.uid = uid; }
    public String getUname() { return this.uname; }
    public void setUname(String uname) { this.uname = uname; }
    public int getUsage() { return this.usage; }
    public void setUsage(int usage) { this.usage = usage; }

    // Default constructor - necessary for instantiation without passing any arguments
    public Usage() {
    }

    // Overriding the toString() method to provide a string representation of Usage objects
    
    public String toString() {
        return "uid: '" + this.uid + "', uame: '" + this.uname + "', usage: '" + this.usage + "'";
    }
}

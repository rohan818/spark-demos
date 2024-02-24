package main.java.ch6;

import java.io.Serializable;

// The UsageCost class models the cost associated with a user's usage, implementing Serializable for Spark's distributed processing capabilities
public class UsageCost implements Serializable {
    // Private properties to encapsulate user's data
    private int uid;                
    private String uname;           
    private int usage;             
    private double cost;            

    // Constructor to initialize a new instance with specified values
    public UsageCost(int uid, String uname, int usage, double cost) {
        this.uid = uid;
        this.uname = uname;
        this.usage = usage;
        this.cost = cost;
    }

    // Getters and Setters
    public int getUid() { return this.uid; }
    public void setUid(int uid) { this.uid = uid; }
    public String getUname() { return this.uname; }
    public void setUname(String uname) { this.uname = uname; }
    public int getUsage() { return this.usage; }
    public void setUsage(int usage) { this.usage = usage; }
    public double getCost() { return this.cost; }
    public void setCost(double cost) { this.cost = cost; }

    public UsageCost() {
    }

    // Override toString() method to provide a string representation of the object
    public String toString() {
        return "uid: '" + this.uid + "', uname: '" + this.uname + "', usage: '" + this.usage + "', cost: '" + this.cost + "'";
    }
}

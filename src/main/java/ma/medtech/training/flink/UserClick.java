package ma.medtech.training.flink;

import java.io.Serializable;

public class UserClick implements Serializable {
    public String user;
    public long timestamp;

    public UserClick() {} // obligatoire pour POJO

    public UserClick(String user, long timestamp) {
        this.user = user;
        this.timestamp = timestamp;
    }

    public String getUser() {
        return user;
    }

    public long getTimestamp() {
        return timestamp;
    }

    @Override
    public String toString() {
        return user + " @ " + timestamp;
    }
}

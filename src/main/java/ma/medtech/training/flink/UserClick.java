package ma.medtech.training.flink;

import java.io.Serializable;
import java.time.Instant;

public class UserClick implements Serializable {
    public String user;
    public long timestamp; // in milliseconds since epoch

    public UserClick() {
        // Default constructor for serialization
    }
    
    public UserClick(String user, long timestamp) {
        this.user = user;
        this.timestamp = timestamp;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public String getUser() {
        return user;
    }

    @Override
    public String toString() {
        return user + " at " + Instant.ofEpochMilli(timestamp);
    }

    public static UserClick of(String user, long timestamp) {
        return new UserClick(user, timestamp);
    }
}

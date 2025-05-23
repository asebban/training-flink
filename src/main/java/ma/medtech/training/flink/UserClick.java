package ma.medtech.training.flink;

public class UserClick {

    public String userId;
    public long timestamp;

    public UserClick(String userId, long timestamp) {
        this.userId = userId;
        this.timestamp = timestamp;
    }

    public UserClick() {
    }

    public String getUserId() {
        return userId;
    }

    public long getTimestamp() {
        return timestamp;
    }

}

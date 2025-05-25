package ma.medtech.training.flink;

public class CardStatus {
    private String cardNumber;
    private String status; // "BLOCKED" ou "ACTIVE"
    // constructeurs, getters, setters, toString()
    public CardStatus() {
        // Constructeur par défaut
    }
    public CardStatus(String cardNumber, String status) {
        this.cardNumber = cardNumber;
        this.status = status;
    }
    public String getCardNumber() {
        return cardNumber;
    }
    public String getStatus() {
        return status;
    }
    public void setCardNumber(String cardNumber) {
        this.cardNumber = cardNumber;
    }
    public void setStatus(String status) {
        this.status = status;
    }
    @Override
    public String toString() {
        return "CardStatus{" +
                "cardNumber='" + cardNumber + '\'' +
                ", status='" + status + '\'' +
                '}';
    }
}

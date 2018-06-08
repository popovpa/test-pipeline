package utils.exceptions;

public class ETLException extends Exception {

    public ETLException(String message) {
        super(message);
    }

    public ETLException(Throwable cause) {
        super(cause);
    }
}

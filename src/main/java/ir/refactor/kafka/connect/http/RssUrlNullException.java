package ir.refactor.kafka.connect.http;

public class RssUrlNullException extends Exception {

    public RssUrlNullException()
    {
        super();
    }

    public RssUrlNullException(String message)
    {
        super(message);
    }

    public RssUrlNullException(String message, Throwable cause)
    {
        super(message , cause);
    }
    public RssUrlNullException(Throwable cause) {
        super(cause);
    }
    protected RssUrlNullException(String message,
                                  Throwable cause,
                                  boolean enableSuppression,
                                  boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }
}

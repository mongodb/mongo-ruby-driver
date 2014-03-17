package org.mongodb.sasl;

public class MongoSecurityException extends RuntimeException {

    private static final long serialVersionUID = -7531399100914218967L;

    public MongoSecurityException(final Throwable cause) {
        super(cause);
    }

    public MongoSecurityException(final String message) {
        super(message);
    }

    public MongoSecurityException(final String message, final Throwable cause) {
        super(message, cause);
    }
}

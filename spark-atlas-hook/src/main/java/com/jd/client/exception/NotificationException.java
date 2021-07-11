package com.jd.client.exception;

import org.apache.atlas.AtlasException;

import java.util.List;

public class NotificationException extends AtlasException {
    private List<String> failedMessages;

    public NotificationException(Exception e) {
        super(e);
    }

    public NotificationException(Exception e, List<String> failedMessages) {
        super(e);
        this.failedMessages = failedMessages;
    }

    public List<String> getFailedMessages() {
        return failedMessages;
    }
}

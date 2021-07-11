package com.jd.client;

import org.apache.atlas.notification.NotificationException;

import java.util.List;

public interface NotificationInterface {
    enum NotificationType {
        HOOK;
    }
    void setCurrentUser(String user);
    <T> void send(NotificationInterface.NotificationType type, List<T> messages) throws NotificationException;
    void close();

}

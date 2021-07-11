package com.jd.client;



import com.jd.client.exception.NotificationException;

import java.util.List;

public interface NotificationInterface {
    enum NotificationType {
        HOOK;
    }
    void setCurrentUser(String user);
    <T> void send(NotificationInterface.NotificationType type, List<T> messages) throws NotificationException;
    void close();
}

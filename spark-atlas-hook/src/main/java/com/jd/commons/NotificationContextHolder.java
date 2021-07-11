package com.jd.commons;
public class NotificationContextHolder {
    private static final ThreadLocal<String> MESSAGE_KEY=new ThreadLocal<String>();

    public static void setMessageKey(String key){
        MESSAGE_KEY.set(key);
    }
    public static String getMessagaKey(){
        return MESSAGE_KEY.get();
    }
    public static void clear(){
        MESSAGE_KEY.remove();
    }
}
package com.jd.client;


import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import org.apache.atlas.model.notification.MessageVersion;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlRootElement;
import java.util.Date;

import static com.fasterxml.jackson.annotation.JsonAutoDetect.Visibility.NONE;
import static com.fasterxml.jackson.annotation.JsonAutoDetect.Visibility.PUBLIC_ONLY;

/**
 * Represents a notification message that is associated with a version.
 */
@JsonAutoDetect(getterVisibility=PUBLIC_ONLY, setterVisibility=PUBLIC_ONLY, fieldVisibility=NONE)
@JsonSerialize(include=JsonSerialize.Inclusion.NON_NULL)
@JsonIgnoreProperties(ignoreUnknown=true)
@XmlRootElement
@XmlAccessorType(XmlAccessType.PROPERTY)
public class AtlasNotificationMessage<T> extends AtlasNotificationBaseMessage {
    private String msgSourceIP;
    private String msgCreatedBy;
    private long   msgCreationTime;

    /**
     * The actual message.
     */
    private T message;


    // ----- Constructors ----------------------------------------------------
    public AtlasNotificationMessage() {
    }

    public AtlasNotificationMessage(MessageVersion version, T message) {
        this(version, message, null, null);
    }

    public AtlasNotificationMessage(MessageVersion version, T message, String msgSourceIP, String createdBy) {
        super(version);

        this.msgSourceIP     = msgSourceIP;
        this.msgCreatedBy    = createdBy;
        this.msgCreationTime = (new Date()).getTime();
        this.message         = message;
    }


    public String getMsgSourceIP() {
        return msgSourceIP;
    }

    public void setMsgSourceIP(String msgSourceIP) {
        this.msgSourceIP = msgSourceIP;
    }

    public String getMsgCreatedBy() {
        return msgCreatedBy;
    }

    public void setMsgCreatedBy(String msgCreatedBy) {
        this.msgCreatedBy = msgCreatedBy;
    }

    public long getMsgCreationTime() {
        return msgCreationTime;
    }

    public void setMsgCreationTime(long msgCreationTime) {
        this.msgCreationTime = msgCreationTime;
    }

    public T getMessage() {
        return message;
    }

    public void setMessage(T message) {
        this.message = message;
    }
}

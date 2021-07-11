package com.jd.client.model;


import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlRootElement;

import static com.fasterxml.jackson.annotation.JsonAutoDetect.Visibility.NONE;
import static com.fasterxml.jackson.annotation.JsonAutoDetect.Visibility.PUBLIC_ONLY;

@JsonAutoDetect(getterVisibility=PUBLIC_ONLY, setterVisibility=PUBLIC_ONLY, fieldVisibility=NONE)
@JsonSerialize(include=JsonSerialize.Inclusion.NON_NULL)
@JsonIgnoreProperties(ignoreUnknown=true)
@XmlRootElement
@XmlAccessorType(XmlAccessType.PROPERTY)
public class AtlasNotificationStringMessage extends AtlasNotificationBaseMessage {
    private String message = null;

    public AtlasNotificationStringMessage() {
        super(MessageVersion.CURRENT_VERSION);
    }

    public AtlasNotificationStringMessage(String message) {
        super(MessageVersion.CURRENT_VERSION);

        this.message = message;
    }

    public AtlasNotificationStringMessage(String message, String msgId, CompressionKind compressionKind) {
        super(MessageVersion.CURRENT_VERSION, msgId, compressionKind);

        this.message = message;
    }

    public AtlasNotificationStringMessage(String message, String msgId, CompressionKind compressionKind, int msgSplitIdx, int msgSplitCount) {
        super(MessageVersion.CURRENT_VERSION, msgId, compressionKind, msgSplitIdx, msgSplitCount);

        this.message = message;
    }

    public AtlasNotificationStringMessage(byte[] encodedBytes, String msgId, CompressionKind compressionKind) {
        super(MessageVersion.CURRENT_VERSION, msgId, compressionKind);

        this.message = AtlasNotificationBaseMessage.getStringUtf8(encodedBytes);
    }

    public AtlasNotificationStringMessage(byte[] encodedBytes, int offset, int length, String msgId, CompressionKind compressionKind, int msgSplitIdx, int msgSplitCount) {
        super(MessageVersion.CURRENT_VERSION, msgId, compressionKind, msgSplitIdx, msgSplitCount);

        this.message = new String(encodedBytes, offset, length);
    }

    public void setMessage(String message) {
        this.message = message;
    }

    public String getMessage() {
        return message;
    }
}
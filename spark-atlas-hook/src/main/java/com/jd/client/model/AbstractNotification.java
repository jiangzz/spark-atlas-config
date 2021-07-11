package com.jd.client.model;

import com.jd.client.NotificationInterface;
import com.jd.client.exception.NotificationException;
import org.apache.atlas.type.AtlasType;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.Inet4Address;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;

import static com.jd.client.model.AtlasNotificationBaseMessage.MESSAGE_COMPRESSION_ENABLED;
import static com.jd.client.model.AtlasNotificationBaseMessage.MESSAGE_MAX_LENGTH_BYTES;


public abstract class AbstractNotification implements NotificationInterface {
    private static final Logger LOG = LoggerFactory.getLogger(AbstractNotification.class);
    private static String        msgIdPrefix = UUID.randomUUID().toString();
    private static AtomicInteger msgIdSuffix = new AtomicInteger(0);
    public static final MessageVersion CURRENT_MESSAGE_VERSION = new MessageVersion("1.0.0");
    public static final int MAX_BYTES_PER_CHAR = 4;  // each char can encode upto 4 bytes in UTF-8

    private static String localHostAddress = "";

    private static String currentUser = "";
    @Override
    public <T> void send(NotificationInterface.NotificationType type, List<T> messages) throws NotificationException {
        List<String> strMessages = new ArrayList<>(messages.size());

        for (int index = 0; index < messages.size(); index++) {
            createNotificationMessages(messages.get(index), strMessages);
        }
        sendInternal(type, strMessages);
    }
    @Override
    public void setCurrentUser(String user) {
        currentUser = user;
    }
    protected abstract void sendInternal(NotificationInterface.NotificationType type, List<String> messages) throws NotificationException;
    private static String getHostAddress() {
        if (StringUtils.isEmpty(localHostAddress)) {
            try {
                localHostAddress =  Inet4Address.getLocalHost().getHostAddress();
            } catch (UnknownHostException e) {
                LOG.warn("failed to get local host address", e);

                localHostAddress =  "";
            }
        }

        return localHostAddress;
    }
    private static String getCurrentUser() {
        return currentUser;
    }
    public static void createNotificationMessages(Object message, List<String> msgJsonList) {
       AtlasNotificationMessage<?> notificationMsg = new AtlasNotificationMessage(CURRENT_MESSAGE_VERSION, message, getHostAddress(), getCurrentUser());
        String                      msgJson         = AtlasType.toV1Json(notificationMsg);

        boolean msgLengthExceedsLimit = (msgJson.length() * MAX_BYTES_PER_CHAR) > MESSAGE_MAX_LENGTH_BYTES;

        if (msgLengthExceedsLimit) { // get utf-8 bytes for msgJson and check for length limit again
            byte[] msgBytes = AtlasNotificationBaseMessage.getBytesUtf8(msgJson);

            msgLengthExceedsLimit = msgBytes.length > MESSAGE_MAX_LENGTH_BYTES;

            if (msgLengthExceedsLimit) {
                String          msgId           = getNextMessageId();
                AtlasNotificationBaseMessage.CompressionKind compressionKind = AtlasNotificationBaseMessage.CompressionKind.NONE;

                if (MESSAGE_COMPRESSION_ENABLED) {
                    byte[] encodedBytes = AtlasNotificationBaseMessage.gzipCompressAndEncodeBase64(msgBytes);

                    compressionKind = AtlasNotificationBaseMessage.CompressionKind.GZIP;

                    LOG.info("Compressed large message: msgID={}, uncompressed={} bytes, compressed={} bytes", msgId, msgBytes.length, encodedBytes.length);

                    msgLengthExceedsLimit = encodedBytes.length > MESSAGE_MAX_LENGTH_BYTES;

                    if (!msgLengthExceedsLimit) { // no need to split
                        AtlasNotificationStringMessage compressedMsg = new AtlasNotificationStringMessage(encodedBytes, msgId, compressionKind);

                        msgJson  = AtlasType.toV1Json(compressedMsg); // msgJson will not have multi-byte characters here, due to use of encodeBase64() above
                        msgBytes = null; // not used after this point
                    } else { // encodedBytes will be split
                        msgJson  = null; // not used after this point
                        msgBytes = encodedBytes;
                    }
                }

                if (msgLengthExceedsLimit) {
                    // compressed messages are already base64-encoded
                    byte[] encodedBytes = MESSAGE_COMPRESSION_ENABLED ? msgBytes : AtlasNotificationBaseMessage.encodeBase64(msgBytes);
                    int    splitCount   = encodedBytes.length / MESSAGE_MAX_LENGTH_BYTES;

                    if ((encodedBytes.length % MESSAGE_MAX_LENGTH_BYTES) != 0) {
                        splitCount++;
                    }

                    for (int i = 0, offset = 0; i < splitCount; i++) {
                        int length = MESSAGE_MAX_LENGTH_BYTES;

                        if ((offset + length) > encodedBytes.length) {
                            length = encodedBytes.length - offset;
                        }

                        AtlasNotificationStringMessage splitMsg = new AtlasNotificationStringMessage(encodedBytes, offset, length, msgId, compressionKind, i, splitCount);

                        String splitMsgJson = AtlasType.toV1Json(splitMsg);

                        msgJsonList.add(splitMsgJson);

                        offset += length;
                    }

                    LOG.info("Split large message: msgID={}, splitCount={}, length={} bytes", msgId, splitCount, encodedBytes.length);
                }
            }
        }

        if (!msgLengthExceedsLimit) {
            msgJsonList.add(msgJson);
        }
    }
    private static String getNextMessageId() {
        String nextMsgIdPrefix = msgIdPrefix;
        int    nextMsgIdSuffix = msgIdSuffix.getAndIncrement();

        if (nextMsgIdSuffix == Short.MAX_VALUE) { // get a new UUID after 32,767 IDs
            msgIdPrefix = UUID.randomUUID().toString();
            msgIdSuffix = new AtomicInteger(0);
        }
        return nextMsgIdPrefix + "_" + Integer.toString(nextMsgIdSuffix);
    }
}

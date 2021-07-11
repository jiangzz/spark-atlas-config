package com.jd.client;
import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import org.apache.atlas.model.notification.MessageVersion;
import org.apache.commons.codec.binary.Base64;
import org.apache.commons.codec.binary.StringUtils;
import org.apache.commons.compress.utils.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlRootElement;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

import static com.fasterxml.jackson.annotation.JsonAutoDetect.Visibility.NONE;
import static com.fasterxml.jackson.annotation.JsonAutoDetect.Visibility.PUBLIC_ONLY;


@JsonAutoDetect(getterVisibility=PUBLIC_ONLY, setterVisibility=PUBLIC_ONLY, fieldVisibility=NONE)
@JsonSerialize(include=JsonSerialize.Inclusion.NON_NULL)
@JsonIgnoreProperties(ignoreUnknown=true)
@XmlRootElement
@XmlAccessorType(XmlAccessType.PROPERTY)
public class AtlasNotificationBaseMessage {
    private static final Logger LOG = LoggerFactory.getLogger(AtlasNotificationBaseMessage.class);

    public static final int     MESSAGE_MAX_LENGTH_BYTES    = 1000 * 1000 - 512; // 512 bytes for envelop;
    public static final boolean MESSAGE_COMPRESSION_ENABLED = true;

    public enum CompressionKind { NONE, GZIP };

    private MessageVersion version            = null;
    private String          msgId              = null;
    private AtlasNotificationBaseMessage.CompressionKind msgCompressionKind = AtlasNotificationBaseMessage.CompressionKind.NONE;
    private int             msgSplitIdx        = 1;
    private int             msgSplitCount      = 1;


    public AtlasNotificationBaseMessage() {
    }

    public AtlasNotificationBaseMessage(MessageVersion version) {
        this(version, null, AtlasNotificationBaseMessage.CompressionKind.NONE);
    }

    public AtlasNotificationBaseMessage(MessageVersion version, String msgId, CompressionKind msgCompressionKind) {
        this.version            = version;
        this.msgId              = msgId;
        this.msgCompressionKind = msgCompressionKind;
    }

    public AtlasNotificationBaseMessage(MessageVersion version, String msgId, AtlasNotificationBaseMessage.CompressionKind msgCompressionKind, int msgSplitIdx, int msgSplitCount) {
        this.version            = version;
        this.msgId              = msgId;
        this.msgCompressionKind = msgCompressionKind;
        this.msgSplitIdx        = msgSplitIdx;
        this.msgSplitCount      = msgSplitCount;
    }

    public void setVersion(MessageVersion version) {
        this.version = version;
    }

    public MessageVersion getVersion() {
        return version;
    }

    public String getMsgId() {
        return msgId;
    }

    public void setMsgId(String msgId) {
        this.msgId = msgId;
    }

    public AtlasNotificationBaseMessage.CompressionKind getMsgCompressionKind() {
        return msgCompressionKind;
    }

    public void setMsgCompressed(AtlasNotificationBaseMessage.CompressionKind msgCompressionKind) {
        this.msgCompressionKind = msgCompressionKind;
    }

    public int getMsgSplitIdx() {
        return msgSplitIdx;
    }

    public void setMsgSplitIdx(int msgSplitIdx) {
        this.msgSplitIdx = msgSplitIdx;
    }

    public int getMsgSplitCount() {
        return msgSplitCount;
    }

    public void setMsgSplitCount(int msgSplitCount) {
        this.msgSplitCount = msgSplitCount;
    }

    /**
     * Compare the version of this message with the given version.
     *
     * @param compareToVersion  the version to compare to
     *
     * @return a negative integer, zero, or a positive integer as this message's version is less than, equal to,
     *         or greater than the given version.
     */
    public int compareVersion(MessageVersion compareToVersion) {
        return version.compareTo(compareToVersion);
    }


    public static byte[] getBytesUtf8(String str) {
        return StringUtils.getBytesUtf8(str);
    }

    public static String getStringUtf8(byte[] bytes) {
        return StringUtils.newStringUtf8(bytes);
    }

    public static byte[] encodeBase64(byte[] bytes) {
        return Base64.encodeBase64(bytes);
    }

    public static byte[] decodeBase64(byte[] bytes) {
        return Base64.decodeBase64(bytes);
    }

    public static byte[] gzipCompressAndEncodeBase64(byte[] bytes) {
        return encodeBase64(gzipCompress(bytes));
    }

    public static byte[] decodeBase64AndGzipUncompress(byte[] bytes) {
        return gzipUncompress(decodeBase64(bytes));
    }

    public static String gzipCompress(String str) {
        byte[] bytes           = getBytesUtf8(str);
        byte[] compressedBytes = gzipCompress(bytes);
        byte[] encodedBytes    = encodeBase64(compressedBytes);

        return getStringUtf8(encodedBytes);
    }

    public static String gzipUncompress(String str) {
        byte[] encodedBytes    = getBytesUtf8(str);
        byte[] compressedBytes = decodeBase64(encodedBytes);
        byte[] bytes           = gzipUncompress(compressedBytes);

        return getStringUtf8(bytes);
    }

    public static byte[] gzipCompress(byte[] content) {
        ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();

        try {
            GZIPOutputStream gzipOutputStream = new GZIPOutputStream(byteArrayOutputStream);

            gzipOutputStream.write(content);
            gzipOutputStream.close();
        } catch (IOException e) {
            LOG.error("gzipCompress(): error compressing {} bytes", content.length, e);

            throw new RuntimeException(e);
        }

        return byteArrayOutputStream.toByteArray();
    }

    public static byte[] gzipUncompress(byte[] content) {
        ByteArrayOutputStream out = new ByteArrayOutputStream();

        try {
            IOUtils.copy(new GZIPInputStream(new ByteArrayInputStream(content)), out);
        } catch (IOException e) {
            LOG.error("gzipUncompress(): error uncompressing {} bytes", content.length, e);
        }

        return out.toByteArray();
    }
}

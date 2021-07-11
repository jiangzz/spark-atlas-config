package com.jd.client;

import avro.shaded.com.google.common.annotations.VisibleForTesting;
import com.jd.client.exception.NotificationException;
import com.jd.client.model.AbstractNotification;
import com.jd.commons.NotificationContextHolder;
import org.apache.atlas.AtlasException;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.alias.CredentialProvider;
import org.apache.hadoop.security.alias.CredentialProviderFactory;
import org.apache.kafka.clients.producer.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.Future;

import static org.apache.atlas.security.SecurityProperties.*;


public class KafkaNotification extends AbstractNotification {
    public static final Logger LOG = LoggerFactory.getLogger(KafkaNotification.class);

    static final String KAFKA_SASL_JAAS_CONFIG_PROPERTY = "sasl.jaas.config";
    private static final String JAAS_CONFIG_PREFIX_PARAM = "atlas.jaas";
    private static final String JAAS_CONFIG_LOGIN_MODULE_NAME_PARAM = "loginModuleName";
    private static final String JAAS_CONFIG_LOGIN_MODULE_CONTROL_FLAG_PARAM = "loginModuleControlFlag";
    private static final String JAAS_DEFAULT_LOGIN_MODULE_CONTROL_FLAG = "required";
    private static final String JAAS_VALID_LOGIN_MODULE_CONTROL_FLAG_OPTIONS = "optional|requisite|sufficient|required";
    private static final String JAAS_CONFIG_LOGIN_OPTIONS_PREFIX = "option";
    private static final String JAAS_PRINCIPAL_PROP = "principal";
    private static final String JAAS_DEFAULT_CLIENT_NAME = "KafkaClient";
    private static final String JAAS_TICKET_BASED_CLIENT_NAME = "ticketBased-KafkaClient";

    public static final String PROPERTY_PREFIX = "atlas.kafka";
    public static final String ATLAS_HOOK_TOPIC = "atlas.hook.topic";
    private  final Map<NotificationType, String> PRODUCER_TOPIC_MAP;
    private final Properties properties;
    private final Map<NotificationType, KafkaProducer> producers = new HashMap<>();

    public KafkaNotification(Properties applicationProperties) throws AtlasException {
        super();
        PRODUCER_TOPIC_MAP=new HashMap<NotificationType, String>();
        PRODUCER_TOPIC_MAP.put(NotificationType.HOOK,applicationProperties.getProperty(ATLAS_HOOK_TOPIC,"ATLAS_HOOK"));

        LOG.info("==> UserDefineKafkaNotification()");

        properties = new Properties();
        applicationProperties.entrySet().stream().filter(entry->entry.getKey().toString().startsWith(PROPERTY_PREFIX)).forEach(entry->{
            properties.put(entry.getKey().toString().replace(PROPERTY_PREFIX+".",""),entry.getValue());
        });
        //Override default configs
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        boolean oldApiCommitEnableFlag = Boolean.valueOf(properties.getProperty("auto.commit.enable", "false"));
        properties.put("session.timeout.ms", properties.getProperty("session.timeout.ms", "30000"));
        if ( Boolean.valueOf(properties.getProperty(TLS_ENABLED, "false"))) {
            try {
                properties.put("ssl.truststore.password", getPassword(properties, TRUSTSTORE_PASSWORD_KEY));
            } catch (Exception e) {
                LOG.error("Exception while getpassword truststore.password ", e);
            }
        }
        setKafkaJAASProperties(applicationProperties, properties);
        LOG.info("<== UserDefineKafkaNotification()");
    }
    public static String getPassword(Properties config, String key) throws IOException {

        String password;

        String provider = config.getProperty(CERT_STORES_CREDENTIAL_PROVIDER_PATH);
        if (provider != null) {
            LOG.info("Attempting to retrieve password for key {} from configured credential provider path {}", key, provider);
            org.apache.hadoop.conf.Configuration c = new org.apache.hadoop.conf.Configuration();
            c.set(CredentialProviderFactory.CREDENTIAL_PROVIDER_PATH, provider);
            CredentialProvider credentialProvider = CredentialProviderFactory.getProviders(c).get(0);
            CredentialProvider.CredentialEntry entry = credentialProvider.getCredentialEntry(key);
            if (entry == null) {
                throw new IOException(String.format("No credential entry found for %s. "
                        + "Please create an entry in the configured credential provider", key));
            } else {
                password = String.valueOf(entry.getCredential());
            }

        } else {
            throw new IOException("No credential provider path configured for storage of certificate store passwords");
        }

        return password;
    }

    @Override
    public void close() {
        LOG.info("==> KafkaNotification.close()");

        for (KafkaProducer producer : producers.values()) {
            if (producer != null) {
                try {
                    producer.flush();
                    producer.close();
                } catch (Throwable t) {
                    LOG.error("failed to close Kafka producer. Ignoring", t);
                }
            }
        }
        producers.clear();
        LOG.info("<== KafkaNotification.close()");
    }


    // ----- AbstractNotification --------------------------------------------
    @Override
    public void sendInternal(NotificationType notificationType, List<String> messages) throws NotificationException {
        KafkaProducer producer = getOrCreateProducer(notificationType);
        sendInternalToProducer(producer, notificationType, messages);
    }

    void sendInternalToProducer(Producer p, NotificationType notificationType, List<String> messages) throws NotificationException {
        String topic = PRODUCER_TOPIC_MAP.get(notificationType);

        List<MessageContext> messageContexts = new ArrayList<>();

        for (String message : messages) {
            ProducerRecord record=null;
            String messageKey = NotificationContextHolder.getMessagaKey();
            if(messageKey!=null){

                record= new ProducerRecord(topic, messageKey,message);
            }else{
                record= new ProducerRecord(topic, message);
            }
            LOG.info("发送消息key:{}\tvalue:{}成功！",messageKey,message);
            if (LOG.isDebugEnabled()) {
                LOG.debug("Sending message for topic {}: {} \t {}", topic,messageKey, message);
            }
            Future future = p.send(record);
            messageContexts.add(new MessageContext(future, message));
        }

        List<String> failedMessages = new ArrayList<>();
        Exception lastFailureException = null;

        for (MessageContext context : messageContexts) {
            try {
                RecordMetadata response = context.getFuture().get();

                if (LOG.isDebugEnabled()) {
                    LOG.debug("Sent message for topic - {}, partition - {}, offset - {}", response.topic(), response.partition(), response.offset());
                }
            } catch (Exception e) {
                lastFailureException = e;

                failedMessages.add(context.getMessage());
            }
        }

        if (lastFailureException != null) {
            throw new NotificationException(lastFailureException, failedMessages);
        }
    }

    private KafkaProducer getOrCreateProducer(NotificationType notificationType) {
        LOG.debug("==> UserDefineKafkaNotification.getOrCreateProducer()");

        KafkaProducer ret = producers.get(notificationType);

        if (ret == null) {
            synchronized (this) {
                ret = producers.get(notificationType);

                if (ret == null) {
                    properties.entrySet().stream().map(p->p.getKey().toString()).forEach(k->{
                        LOG.info("{}\t{}",k,properties.getProperty(k));
                    });
                    ret = new KafkaProducer(properties);

                    producers.put(notificationType, ret);
                }
            }
        }

        LOG.debug("<== UserDefineKafkaNotification.getOrCreateProducer()");

        return ret;
    }

    public static String[] trimAndPurge(String[] strings) {
        List<String> ret = new ArrayList<>();

        if (strings != null) {
            for (int i = 0; i < strings.length; i++) {
                String str = StringUtils.trim(strings[i]);

                if (StringUtils.isNotEmpty(str)) {
                    ret.add(str);
                }
            }
        }

        return ret.toArray(new String[ret.size()]);
    }

    private class MessageContext {
        private final Future<RecordMetadata> future;
        private final String message;

        public MessageContext(Future<RecordMetadata> future, String message) {
            this.future = future;
            this.message = message;
        }

        public Future<RecordMetadata> getFuture() {
            return future;
        }

        public String getMessage() {
            return message;
        }
    }

    void setKafkaJAASProperties(Properties configuration, Properties kafkaProperties) {
        LOG.debug("==> UserDefineKafkaNotification.setKafkaJAASProperties()");

        if (kafkaProperties.containsKey(KAFKA_SASL_JAAS_CONFIG_PROPERTY)) {
            LOG.debug("JAAS config is already set, returning");
            return;
        }

        Properties jaasConfig = new Properties();
        configuration.entrySet().stream().filter(entry-> entry.getKey().toString().startsWith(JAAS_CONFIG_PREFIX_PARAM)).forEach(entry->{
            jaasConfig.put(entry.getKey().toString().replace(JAAS_CONFIG_PREFIX_PARAM+".",""),entry.getValue());
        });

        // JAAS Configuration is present then update set those properties in sasl.jaas.config
        if (jaasConfig != null && !jaasConfig.isEmpty()) {
            String jaasClientName = JAAS_DEFAULT_CLIENT_NAME;

            // Required for backward compatability for Hive CLI
            if (!isLoginKeytabBased() && isLoginTicketBased()) {
                LOG.debug("Checking if ticketBased-KafkaClient is set");
                // if ticketBased-KafkaClient property is not specified then use the default client name
                String ticketBasedConfigPrefix = JAAS_CONFIG_PREFIX_PARAM + "." + JAAS_TICKET_BASED_CLIENT_NAME;
                Properties ticketBasedConfig = new Properties();
                configuration.entrySet().stream().filter(entry-> entry.getKey().toString().startsWith(ticketBasedConfigPrefix)).forEach(entry->{
                    jaasConfig.put(entry.getKey().toString().replace(ticketBasedConfigPrefix+".",""),entry.getValue());
                });

                if (ticketBasedConfig != null && !ticketBasedConfig.isEmpty()) {
                    LOG.debug("ticketBased-KafkaClient JAAS configuration is set, using it");

                    jaasClientName = JAAS_TICKET_BASED_CLIENT_NAME;
                } else {
                    LOG.info("UserGroupInformation.isLoginTicketBased is true, but no JAAS configuration found for client {}. Will use JAAS configuration of client {}", JAAS_TICKET_BASED_CLIENT_NAME, jaasClientName);
                }
            }

            String keyPrefix = jaasClientName + ".";
            String keyParam = keyPrefix + JAAS_CONFIG_LOGIN_MODULE_NAME_PARAM;
            String loginModuleName = jaasConfig.getProperty(keyParam);

            if (loginModuleName == null) {
                LOG.error("Unable to add JAAS configuration for client [{}] as it is missing param [{}]. Skipping JAAS config for [{}]", jaasClientName, keyParam, jaasClientName);
                return;
            }

            keyParam = keyPrefix + JAAS_CONFIG_LOGIN_MODULE_CONTROL_FLAG_PARAM;
            String controlFlag = jaasConfig.getProperty(keyParam);

            if (StringUtils.isEmpty(controlFlag)) {
                String validValues = JAAS_VALID_LOGIN_MODULE_CONTROL_FLAG_OPTIONS;
                controlFlag = JAAS_DEFAULT_LOGIN_MODULE_CONTROL_FLAG;
                LOG.warn("Unknown JAAS configuration value for ({}) = [{}], valid value are [{}] using the default value, REQUIRED", keyParam, controlFlag, validValues);
            }
            String optionPrefix = keyPrefix + JAAS_CONFIG_LOGIN_OPTIONS_PREFIX + ".";
            String principalOptionKey = optionPrefix + JAAS_PRINCIPAL_PROP;
            int optionPrefixLen = optionPrefix.length();
            StringBuffer optionStringBuffer = new StringBuffer();
            for (String key : jaasConfig.stringPropertyNames()) {
                if (key.startsWith(optionPrefix)) {
                    String optionVal = jaasConfig.getProperty(key);
                    if (optionVal != null) {
                        optionVal = optionVal.trim();

                        try {
                            if (key.equalsIgnoreCase(principalOptionKey)) {
                                optionVal = org.apache.hadoop.security.SecurityUtil.getServerPrincipal(optionVal, (String) null);
                            }
                        } catch (IOException e) {
                            LOG.warn("Failed to build serverPrincipal. Using provided value:[{}]", optionVal);
                        }

                        optionVal = surroundWithQuotes(optionVal);
                        optionStringBuffer.append(String.format(" %s=%s", key.substring(optionPrefixLen), optionVal));
                    }
                }
            }

            String newJaasProperty = String.format("%s %s %s ;", loginModuleName.trim(), controlFlag, optionStringBuffer.toString());
            kafkaProperties.put(KAFKA_SASL_JAAS_CONFIG_PROPERTY, newJaasProperty);
        }

        LOG.debug("<== UserDefineKafkaNotification.setKafkaJAASProperties()");
    }

    @VisibleForTesting
    boolean isLoginKeytabBased() {
        boolean ret = false;

        try {
            ret = UserGroupInformation.isLoginKeytabBased();
        } catch (Exception excp) {
            LOG.warn("Error in determining keytab for KafkaClient-JAAS config", excp);
        }

        return ret;
    }

    @VisibleForTesting
    boolean isLoginTicketBased() {
        boolean ret = false;

        try {
            ret = UserGroupInformation.isLoginTicketBased();
        } catch (Exception excp) {
            LOG.warn("Error in determining ticket-cache for KafkaClient-JAAS config", excp);
        }

        return ret;
    }

    private static String surroundWithQuotes(String optionVal) {
        if (StringUtils.isEmpty(optionVal)) {
            return optionVal;
        }
        String ret = optionVal;

        // For property values which have special chars like "@" or "/", we need to enclose it in
        // double quotes, so that Kafka can parse it
        // If the property is already enclosed in double quotes, then do nothing.
        if (optionVal.indexOf(0) != '"' && optionVal.indexOf(optionVal.length() - 1) != '"') {
            // If the string as special characters like except _,-
            final String SPECIAL_CHAR_LIST = "/!@#%^&*";
            if (StringUtils.containsAny(optionVal, SPECIAL_CHAR_LIST)) {
                ret = String.format("\"%s\"", optionVal);
            }
        }

        return ret;
    }
}

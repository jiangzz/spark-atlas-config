package com.jd.client;

import org.apache.atlas.AtlasException;
import org.apache.atlas.model.notification.HookNotification;
import org.apache.atlas.notification.NotificationException;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.ShutdownHookManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.security.PrivilegedExceptionAction;
import java.util.List;
import java.util.Properties;


public class AtlasHook {
    private static final Logger LOG = LoggerFactory.getLogger(AtlasHook.class);
    protected static Properties props;
    protected static NotificationInterface notificationInterface;

    public AtlasHook(Properties props) throws AtlasException {
        this.props=props;
        notificationInterface=new KafkaNotification(props);

        String currentUser = "";

        try {
            currentUser = getUser(null,null);
        } catch (Exception excp) {
            LOG.warn("Error in determining current user", excp);
        }

        notificationInterface.setCurrentUser(currentUser);

        ShutdownHookManager.get().addShutdownHook(new Thread() {
            @Override
            public void run() {
                notificationInterface.close();
            }
        },30);
        LOG.info("Created Atlas Spark Hook");
    }
    public static String getUser(String userName, UserGroupInformation ugi) {
        if (StringUtils.isNotEmpty(userName)) {
            if (LOG.isDebugEnabled()) {
                LOG.debug("Returning userName {}", userName);
            }
            return userName;
        }

        if (ugi != null && StringUtils.isNotEmpty(ugi.getShortUserName())) {
            if (LOG.isDebugEnabled()) {
                LOG.debug("Returning ugi.getShortUserName {}", userName);
            }
            return ugi.getShortUserName();
        }

        try {
            return UserGroupInformation.getCurrentUser().getShortUserName();
        } catch (IOException e) {
            LOG.warn("Failed for UserGroupInformation.getCurrentUser() ", e);
            return System.getProperty("user.name");
        }
    }

    protected void notifyEntities(List<HookNotification> messages, UserGroupInformation ugi) {
        notifyEntities(messages, ugi, 3);
    }
    private  void notifyEntities(final List<HookNotification> messages, final UserGroupInformation ugi, final  int maxRetries) {
        notifyEntitiesInternal(messages, maxRetries, ugi, notificationInterface, true);
    }

    private void notifyEntitiesInternal(final List<HookNotification> messages, int maxRetries, UserGroupInformation ugi,
                                       final NotificationInterface notificationInterface,
                                       boolean shouldLogFailedMessages) {
        if (messages == null || messages.isEmpty()) {
            return;
        }

        final int maxAttempts         = maxRetries < 1 ? 1 : maxRetries;
        Exception notificationFailure = null;

        for (int numAttempt = 1; numAttempt <= maxAttempts; numAttempt++) {
            if (numAttempt > 1) { // retry attempt
                try {
                    LOG.info("Sleeping for {} ms before retry", 3000);
                    Thread.sleep(3000);
                } catch (InterruptedException ie) {
                    LOG.error("Notification hook thread sleep interrupted");
                    break;
                }
            }

            try {
                if (ugi == null) {
                    notificationInterface.send(NotificationInterface.NotificationType.HOOK, messages);
                } else {
                    PrivilegedExceptionAction<Object> privilegedNotify = new PrivilegedExceptionAction<Object>() {
                        @Override
                        public Object run() throws Exception {
                            notificationInterface.send(NotificationInterface.NotificationType.HOOK, messages);
                            return messages;
                        }
                    };

                    ugi.doAs(privilegedNotify);
                }

                notificationFailure = null; // notification sent successfully, reset error

                break;
            } catch (Exception e) {
                notificationFailure = e;
                LOG.error("Failed to send notification - attempt #{}; error={}", numAttempt, e.getMessage());
            }
        }
        if (notificationFailure != null) {
            if (shouldLogFailedMessages && notificationFailure instanceof NotificationException) {
                final List<String> failedMessages = ((NotificationException) notificationFailure).getFailedMessages();

                for (String msg : failedMessages) {
                    LOG.error(msg);
                }
            }
            LOG.error("Giving up after {} failed attempts to send notification to Atlas: {}", maxAttempts, messages.toString(), notificationFailure);
        }
    }

}

/**
 * Copyright 2026 IBM Corporation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */
package com.ibm.eventstreams.connect.mqsink.processor;

import com.ibm.msg.client.jms.JmsConstants;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import javax.jms.JMSException;
import javax.jms.Message;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.header.Header;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Copies Kafka Connect headers to JMS message properties.
 *
 * <p>Supports both legacy deployments where headers arrive as strings (for example from older
 * MQ source connector versions) and typed headers produced by schema-aware connectors.
 */
public class KafkaToJmsHeaderConverter {
    private static final Logger log = LoggerFactory.getLogger(KafkaToJmsHeaderConverter.class);

    private static final Set<String> MQMD_INTEGER_PROPERTIES = new HashSet<>(Arrays.asList(
            JmsConstants.JMS_IBM_MQMD_REPORT,
            JmsConstants.JMS_IBM_MQMD_MSGTYPE,
            JmsConstants.JMS_IBM_MQMD_EXPIRY,
            JmsConstants.JMS_IBM_MQMD_FEEDBACK,
            JmsConstants.JMS_IBM_MQMD_ENCODING,
            JmsConstants.JMS_IBM_MQMD_CODEDCHARSETID,
            JmsConstants.JMS_IBM_MQMD_PRIORITY,
            JmsConstants.JMS_IBM_MQMD_PERSISTENCE,
            JmsConstants.JMS_IBM_MQMD_BACKOUTCOUNT,
            JmsConstants.JMS_IBM_MQMD_PUTAPPLTYPE,
            JmsConstants.JMS_IBM_MQMD_MSGSEQNUMBER,
            JmsConstants.JMS_IBM_MQMD_OFFSET,
            JmsConstants.JMS_IBM_MQMD_MSGFLAGS,
            JmsConstants.JMS_IBM_MQMD_ORIGINALLENGTH
    ));

    private static final Set<String> MQMD_STRING_PROPERTIES = new HashSet<>(Arrays.asList(
            JmsConstants.JMS_IBM_MQMD_FORMAT,
            JmsConstants.JMS_IBM_MQMD_REPLYTOQ,
            JmsConstants.JMS_IBM_MQMD_REPLYTOQMGR,
            JmsConstants.JMS_IBM_MQMD_USERIDENTIFIER,
            JmsConstants.JMS_IBM_MQMD_APPLIDENTITYDATA,
            JmsConstants.JMS_IBM_MQMD_PUTAPPLNAME,
            JmsConstants.JMS_IBM_MQMD_PUTDATE,
            JmsConstants.JMS_IBM_MQMD_PUTTIME,
            JmsConstants.JMS_IBM_MQMD_APPLORIGINDATA
    ));

    private static final Set<String> MQMD_BYTES_PROPERTIES_TO_SKIP = new HashSet<>(Arrays.asList(
            JmsConstants.JMS_IBM_MQMD_MSGID,
            JmsConstants.JMS_IBM_MQMD_CORRELID,
            JmsConstants.JMS_IBM_MQMD_ACCOUNTINGTOKEN,
            JmsConstants.JMS_IBM_MQMD_GROUPID
    ));

    private static final Set<String> JMS_IBM_INTEGER_PROPERTIES = new HashSet<>(Arrays.asList(
            JmsConstants.JMS_IBM_REPORT_EXCEPTION,
            JmsConstants.JMS_IBM_REPORT_EXPIRATION,
            JmsConstants.JMS_IBM_REPORT_COA,
            JmsConstants.JMS_IBM_REPORT_COD,
            JmsConstants.JMS_IBM_REPORT_PAN,
            JmsConstants.JMS_IBM_REPORT_NAN,
            JmsConstants.JMS_IBM_REPORT_PASS_MSG_ID,
            JmsConstants.JMS_IBM_REPORT_PASS_CORREL_ID,
            JmsConstants.JMS_IBM_REPORT_DISCARD_MSG,
            JmsConstants.JMS_IBM_MSGTYPE,
            JmsConstants.JMS_IBM_FEEDBACK,
            JmsConstants.JMS_IBM_ENCODING,
            JmsConstants.JMS_IBM_PUTAPPLTYPE,
            JmsConstants.JMS_IBM_CHARACTER_SET
    ));

    private static final Set<String> JMS_IBM_STRING_PROPERTIES = new HashSet<>(Arrays.asList(
            JmsConstants.JMS_IBM_FORMAT
    ));

    private static final Set<String> JMS_IBM_BOOLEAN_PROPERTIES = new HashSet<>(Arrays.asList(
            JmsConstants.JMS_IBM_LAST_MSG_IN_GROUP
    ));

    /**
     * Copies a single Kafka header to a JMS message property.
     *
     * @param message the JMS message
     * @param header  the Kafka header
     * @throws JMSException if the property cannot be set
     */
    public void copyHeaderToJmsProperty(final Message message, final Header header) throws JMSException {
        final String key = header.key();
        final Object value = header.value();

        if (value == null) {
            log.debug("Skipping null value for header '{}'", key);
            return;
        }

        if (MQMD_BYTES_PROPERTIES_TO_SKIP.contains(key)) {
            log.debug("Skipping byte array MQMD header '{}' - use MQMD API instead", key);
            return;
        }

        if (isIntegerProperty(key)) {
            message.setObjectProperty(key, coerceToInteger(value, key));
            return;
        }

        if (isStringProperty(key)) {
            message.setObjectProperty(key, value.toString());
            return;
        }

        if (isBooleanProperty(key)) {
            message.setObjectProperty(key, coerceToBoolean(value, key));
            return;
        }

        setGeneralProperty(message, key, value, header.schema());
    }

    private static boolean isIntegerProperty(final String key) {
        return MQMD_INTEGER_PROPERTIES.contains(key) || JMS_IBM_INTEGER_PROPERTIES.contains(key);
    }

    private static boolean isStringProperty(final String key) {
        return MQMD_STRING_PROPERTIES.contains(key) || JMS_IBM_STRING_PROPERTIES.contains(key);
    }

    private static boolean isBooleanProperty(final String key) {
        return JMS_IBM_BOOLEAN_PROPERTIES.contains(key);
    }

    private static Integer coerceToInteger(final Object value, final String key) {
        if (value instanceof Integer) {
            return (Integer) value;
        }
        if (value instanceof Number) {
            return Integer.valueOf(((Number) value).intValue());
        }
        if (value instanceof String) {
            try {
                return Integer.valueOf((String) value);
            } catch (final NumberFormatException e) {
                throw new ConnectException(
                        "Header '" + key + "' requires an integer value but got '" + value + "'", e);
            }
        }
        throw new ConnectException("Header '" + key + "' requires an integer-compatible value but got "
                + value.getClass().getName());
    }

    private static Boolean coerceToBoolean(final Object value, final String key) {
        if (value instanceof Boolean) {
            return (Boolean) value;
        }
        if (value instanceof Integer) {
            return Boolean.valueOf(((Integer) value) != 0);
        }
        if (value instanceof String) {
            final String strValue = ((String) value).trim();
            if ("1".equals(strValue)) {
                return Boolean.TRUE;
            }
            if ("0".equals(strValue)) {
                return Boolean.FALSE;
            }
            if ("true".equalsIgnoreCase(strValue) || "false".equalsIgnoreCase(strValue)) {
                return Boolean.valueOf(Boolean.parseBoolean(strValue));
            }
            throw new ConnectException("Header '" + key + "' requires a boolean value but got '" + value + "'");
        }
        throw new ConnectException("Header '" + key + "' requires a boolean-compatible value but got "
                + value.getClass().getName());
    }

    private static void setGeneralProperty(final Message message, final String key, final Object value,
            final Schema schema) throws JMSException {
        if (schema != null) {
            switch (schema.type()) {
                case INT8:
                    message.setByteProperty(key, ((Number) value).byteValue());
                    return;
                case INT16:
                    message.setShortProperty(key, ((Number) value).shortValue());
                    return;
                case INT32:
                    message.setIntProperty(key, ((Number) value).intValue());
                    return;
                case INT64:
                    message.setLongProperty(key, ((Number) value).longValue());
                    return;
                case FLOAT32:
                    message.setFloatProperty(key, ((Number) value).floatValue());
                    return;
                case FLOAT64:
                    message.setDoubleProperty(key, ((Number) value).doubleValue());
                    return;
                case BOOLEAN:
                    message.setBooleanProperty(key, (Boolean) value);
                    return;
                case STRING:
                    message.setStringProperty(key, (String) value);
                    return;
                case BYTES:
                    message.setObjectProperty(key, value);
                    return;
                default:
                    log.debug("Unsupported header schema type {} for '{}', using string fallback", schema.type(), key);
                    break;
            }
        }

        if (value instanceof Byte) {
            message.setByteProperty(key, (Byte) value);
        } else if (value instanceof Short) {
            message.setShortProperty(key, (Short) value);
        } else if (value instanceof Integer) {
            message.setIntProperty(key, (Integer) value);
        } else if (value instanceof Long) {
            message.setLongProperty(key, (Long) value);
        } else if (value instanceof Float) {
            message.setFloatProperty(key, (Float) value);
        } else if (value instanceof Double) {
            message.setDoubleProperty(key, (Double) value);
        } else if (value instanceof Boolean) {
            message.setBooleanProperty(key, (Boolean) value);
        } else if (value instanceof String) {
            message.setStringProperty(key, (String) value);
        } else if (value instanceof byte[]) {
            message.setObjectProperty(key, value);
        } else {
            message.setStringProperty(key, value.toString());
            log.debug("Set header '{}' as string fallback: {}", key, value);
        }
    }
}

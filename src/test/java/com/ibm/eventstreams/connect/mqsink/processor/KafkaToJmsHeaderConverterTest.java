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

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertThrows;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

import javax.jms.Message;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.header.ConnectHeaders;
import org.apache.kafka.connect.header.Header;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import com.ibm.msg.client.jms.JmsConstants;

@RunWith(MockitoJUnitRunner.class)
public class KafkaToJmsHeaderConverterTest {

    @Mock
    private Message message;

    @InjectMocks
    private KafkaToJmsHeaderConverter converter;

    @Test
    public void copiesLegacyStringMqmdIntegerHeader() throws Exception {
        final Header header = new ConnectHeaders().addString(JmsConstants.JMS_IBM_MQMD_PRIORITY, "5")
                .lastWithName(JmsConstants.JMS_IBM_MQMD_PRIORITY);

        converter.copyHeaderToJmsProperty(message, header);

        verify(message).setObjectProperty(JmsConstants.JMS_IBM_MQMD_PRIORITY, 5);
    }

    @Test
    public void copiesTypedMqmdIntegerHeader() throws Exception {
        final Header header = new ConnectHeaders().addInt(JmsConstants.JMS_IBM_MQMD_PRIORITY, 5)
                .lastWithName(JmsConstants.JMS_IBM_MQMD_PRIORITY);

        converter.copyHeaderToJmsProperty(message, header);

        verify(message).setObjectProperty(JmsConstants.JMS_IBM_MQMD_PRIORITY, 5);
    }

    @Test
    public void rejectsInvalidLegacyMqmdIntegerHeader() throws Exception {
        final Header header = new ConnectHeaders().addString(JmsConstants.JMS_IBM_MQMD_PRIORITY, "abc")
                .lastWithName(JmsConstants.JMS_IBM_MQMD_PRIORITY);

        assertThrows(ConnectException.class, () -> converter.copyHeaderToJmsProperty(message, header));
    }

    @Test
    public void copiesTypedCustomIntegerHeader() throws Exception {
        final Header header = new ConnectHeaders().addInt("priority", 5).lastWithName("priority");

        converter.copyHeaderToJmsProperty(message, header);

        verify(message).setIntProperty("priority", 5);
    }

    @Test
    public void copiesLegacyStringCustomHeader() throws Exception {
        final Header header = new ConnectHeaders().addString("customHeader", "headerValue")
                .lastWithName("customHeader");

        converter.copyHeaderToJmsProperty(message, header);

        verify(message).setStringProperty("customHeader", "headerValue");
    }

    @Test
    public void copiesLegacyStringNumericCustomHeader() throws Exception {
        final Header header = new ConnectHeaders().addString("volume", "11").lastWithName("volume");

        converter.copyHeaderToJmsProperty(message, header);

        verify(message).setStringProperty("volume", "11");
    }

    @Test
    public void copiesTypedBooleanHeader() throws Exception {
        final Header header = new ConnectHeaders().addBoolean("isActive", true).lastWithName("isActive");

        converter.copyHeaderToJmsProperty(message, header);

        verify(message).setBooleanProperty("isActive", true);
    }

    @Test
    public void copiesTypedByteArrayHeader() throws Exception {
        final byte[] value = new byte[] {0x01, 0x02};
        final Header header = new ConnectHeaders().add("customBytes", value, Schema.OPTIONAL_BYTES_SCHEMA)
                .lastWithName("customBytes");

        converter.copyHeaderToJmsProperty(message, header);

        final ArgumentCaptor<Object> captor = ArgumentCaptor.forClass(Object.class);
        verify(message).setObjectProperty(eq("customBytes"), captor.capture());
        assertArrayEquals(value, (byte[]) captor.getValue());
    }

    @Test
    public void skipsMqmdByteArrayHeader() throws Exception {
        final byte[] value = new byte[] {0x01, 0x02};
        final Header header = new ConnectHeaders()
                .add(JmsConstants.JMS_IBM_MQMD_MSGID, value, Schema.OPTIONAL_BYTES_SCHEMA)
                .lastWithName(JmsConstants.JMS_IBM_MQMD_MSGID);

        converter.copyHeaderToJmsProperty(message, header);

        verify(message, never()).setObjectProperty(JmsConstants.JMS_IBM_MQMD_MSGID, value);
        verify(message, never()).setStringProperty(JmsConstants.JMS_IBM_MQMD_MSGID, value.toString());
    }

    @Test
    public void coercesMqmdIntegerHeaderFromLongSchema() throws Exception {
        final Header header = new ConnectHeaders().addLong(JmsConstants.JMS_IBM_MQMD_PRIORITY, 5L)
                .lastWithName(JmsConstants.JMS_IBM_MQMD_PRIORITY);

        converter.copyHeaderToJmsProperty(message, header);

        verify(message).setObjectProperty(JmsConstants.JMS_IBM_MQMD_PRIORITY, 5);
    }

    @Test
    public void coercesIbmBooleanHeaderFromString() throws Exception {
        final Header header = new ConnectHeaders().addString(JmsConstants.JMS_IBM_LAST_MSG_IN_GROUP, "true")
                .lastWithName(JmsConstants.JMS_IBM_LAST_MSG_IN_GROUP);

        converter.copyHeaderToJmsProperty(message, header);

        verify(message).setObjectProperty(JmsConstants.JMS_IBM_LAST_MSG_IN_GROUP, true);
    }

    @Test
    public void classifiesCharacterSetAsInteger() throws Exception {
        final Header header = new ConnectHeaders().addInt(JmsConstants.JMS_IBM_CHARACTER_SET, 819)
                .lastWithName(JmsConstants.JMS_IBM_CHARACTER_SET);

        converter.copyHeaderToJmsProperty(message, header);

        verify(message).setObjectProperty(JmsConstants.JMS_IBM_CHARACTER_SET, 819);
    }
}

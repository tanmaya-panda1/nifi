/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.nifi.snmp.factory.core;

import org.apache.nifi.snmp.configuration.SNMPConfiguration;
import org.apache.nifi.util.StringUtils;
import org.junit.jupiter.api.Test;
import org.snmp4j.CommunityTarget;
import org.snmp4j.Snmp;
import org.snmp4j.Target;
import org.snmp4j.security.SecurityLevel;

import static org.apache.nifi.snmp.helper.configurations.SNMPConfigurationFactory.LOCALHOST;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.IsInstanceOf.instanceOf;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;

class V1V2cSNMPFactoryTest extends SNMPSocketSupport {

    private static final int RETRIES = 3;

    @Test
    void testFactoryCreatesV1V2Configuration() {
        final V1V2cSNMPFactory snmpFactory = new V1V2cSNMPFactory();
        final Target target = createInstanceWithRetries(snmpFactory::createTargetInstance, 5);

        assertThat(target, instanceOf(CommunityTarget.class));
        assertNotNull(target.getAddress().toString());
        assertEquals(RETRIES, target.getRetries());
        assertEquals(1, target.getSecurityLevel());
        assertEquals(StringUtils.EMPTY, target.getSecurityName().toString());
    }

    @Test
    void testFactoryCreatesSnmpManager() {
        final V1V2cSNMPFactory snmpFactory = new V1V2cSNMPFactory();
        final Snmp snmpManager = createInstanceWithRetries(snmpFactory::createSnmpManagerInstance, 5);
        final String address = snmpManager.getMessageDispatcher().getTransportMappings().iterator().next().getListenAddress().toString();
        assertNotNull(address);
    }

    @Test
    void testFactoryCreatesResourceHandler() {
        final V1V2cSNMPFactory snmpFactory = spy(V1V2cSNMPFactory.class);
        final SNMPConfiguration snmpConfiguration = getSnmpConfiguration(0, "48");

        snmpFactory.createSNMPResourceHandler(snmpConfiguration);

        verify(snmpFactory).createTargetInstance(snmpConfiguration);
        verify(snmpFactory).createSnmpManagerInstance(snmpConfiguration);
    }

    @Override
    protected SNMPConfiguration getSnmpConfiguration(int managerPort, String targetPort) {
        return new SNMPConfiguration.Builder()
                .setRetries(RETRIES)
                .setManagerPort(managerPort)
                .setTargetHost(LOCALHOST)
                .setTargetPort(targetPort)
                .setSecurityLevel(SecurityLevel.noAuthNoPriv.name())
                .build();
    }
}

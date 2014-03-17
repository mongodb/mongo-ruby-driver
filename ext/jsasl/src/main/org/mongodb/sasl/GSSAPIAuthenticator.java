/*
 * Copyright (C) 2009-2014 MongoDB, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.mongodb.sasl;

import org.ietf.jgss.GSSCredential;
import org.ietf.jgss.GSSException;
import org.ietf.jgss.GSSManager;
import org.ietf.jgss.GSSName;
import org.ietf.jgss.Oid;
import org.jruby.Ruby;
import org.jruby.RubyString;
import org.jruby.RubyBoolean;

import javax.security.sasl.Sasl;
import javax.security.sasl.SaslClient;
import javax.security.sasl.SaslException;
import java.net.UnknownHostException;
import java.net.InetAddress;
import java.util.HashMap;
import java.util.Map;

/**
 * A helper class for SASL authentication using GSSAPI (Kerberos)
 */
public class GSSAPIAuthenticator {
    private static final String GSSAPI_MECHANISM_NAME = "GSSAPI";
    private static final String GSSAPI_OID = "1.2.840.113554.1.2.2";
    public static final String CANONICALIZE_HOST_NAME_KEY = "CANONICALIZE_HOST_NAME";

    private final Ruby runTime;
    private final String userName;
    private final String hostName;
    private final String serviceName;
    private final boolean canonicalizeHostName;

    private final SaslClient saslClient;

    /**
     * Constructs a wrapper for a Sasl client that handles GSSAPI (Kerberos) mechanism authentication.
     *
     * @param runTime the Ruby run time
     * @param userName the user name
     * @param hostName the host name
     * @param serviceName the service name
     * @param canonicalizeHostName whether the hostname should be canonicalized
     */
    public GSSAPIAuthenticator(final Ruby runTime, final RubyString userName, final RubyString hostName, final RubyString serviceName, final RubyBoolean canonicalizeHostName) {
        this.runTime = runTime;
        this.userName = userName.decodeString();
        this.hostName = hostName.decodeString();
        this.serviceName = serviceName.decodeString();
        this.canonicalizeHostName = (Boolean) canonicalizeHostName.toJava(Boolean.class);
        this.saslClient = createSaslClient();
    }

    /**
     * If the mechanism has an initial response, evaluteChallenge() is called to get the challenge. Otherwise, null is returned.
     *
     * @return the initial challenge to send to the server or null if the mechanism doesn't have an initial response.
     *
     * @throws MongoSecurityException if there is no response to the challenge.
     */
    public RubyString initializeChallenge() {
        try {
            return saslClient.hasInitialResponse() ? RubyString.newString(runTime, saslClient.evaluateChallenge(new byte[0])) : null;
        } catch (SaslException e) {
            throw new MongoSecurityException("SASL protocol error: no client response to challenge for credential", e);
        }
    }

    /**
     * Evaluate the next challenge, given the response from the server.
     *
     * @param rubyPayload the non-null challenge sent from the server.
     *
     * @return the response to the challenge
     */
    public RubyString evaluateChallenge(RubyString rubyPayload) {
        try {
            return RubyString.newString(runTime, saslClient.evaluateChallenge(rubyPayload.getBytes()));
        } catch (SaslException e) {
            throw new MongoSecurityException("SASL protocol error: no client response to challenge for credential", e);
        }
    }

    private SaslClient createSaslClient() {
        try {
            Map<String, Object> props = new HashMap<String, Object>();
            props.put(Sasl.CREDENTIALS, getGSSCredential(userName));
            SaslClient saslClient = Sasl.createSaslClient(new String[]{GSSAPI_MECHANISM_NAME}, userName,
                                                          serviceName,
                                                          getHostName(), props, null);
            if (saslClient == null) {
                throw new MongoSecurityException(String.format("No platform support for %s mechanism", GSSAPI_MECHANISM_NAME));
            }
            return saslClient;
        } catch (SaslException e) {
            throw new MongoSecurityException(e);
        } catch (GSSException e) {
            throw new MongoSecurityException(e);
        } catch (UnknownHostException e) {
            throw new MongoSecurityException(e);
        } catch (SecurityException e) {
            throw new MongoSecurityException(e);
        }
    }

    private GSSCredential getGSSCredential(final String userName) throws GSSException {
        Oid krb5Mechanism = new Oid(GSSAPI_OID);
        GSSManager manager = GSSManager.getInstance();
        GSSName name = manager.createName(userName, GSSName.NT_USER_NAME);
        return manager.createCredential(name, GSSCredential.INDEFINITE_LIFETIME, krb5Mechanism, GSSCredential.INITIATE_ONLY);
    }

    private String getHostName() throws UnknownHostException {
        return canonicalizeHostName ? InetAddress.getByName(hostName).getCanonicalHostName() : hostName;
    }
}
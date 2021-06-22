/*
 * This file is part of OpenTSDB.
 *  Copyright (C) 2021 Yahoo.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express  implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package net.opentsdb.horizon.ssl;

import javax.net.ssl.KeyManager;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManager;
import java.security.KeyStore;
import java.util.Map;

public class KeyStoreSSLContextFactory implements SSLContextFactory {

  public static final String KEYSTORE_PATH = "keyStorePath";
  public static final String KEYSTORE_PASSWORD = "keyStorePassword";
  public static final String TRUSTSTORE_PATH = "trustStorePath";
  public static final String TRUSTSTORE_PASSWORD = "trustStorePassword";

  @Override
  public SSLContext createSSLContext(Map<String, Object> initParams) throws Exception {
    final KeyStore keystore =
        KeyStoreUtil.loadKeyStore(
            (String) initParams.get(KEYSTORE_PATH), (String) initParams.get(KEYSTORE_PASSWORD));

    // initialize a key manager to pass to the SSL context using the keystore.
    final KeyManager[] keyManagers =
        KeyStoreUtil.getKeyManagers(keystore, (String) initParams.get(KEYSTORE_PASSWORD));

    // trust manager with null value, will use java's default trust store
    TrustManager[] trustManagers = null;
    String trustStore = (String) initParams.get(TRUSTSTORE_PATH);
    if (trustStore != null && !trustStore.isEmpty()) {
      trustManagers =
          KeyStoreUtil.getTrustManagers(trustStore, (String) initParams.get(TRUSTSTORE_PASSWORD));
    }

    return KeyStoreUtil.buildSSLContext(keyManagers, trustManagers);
  }
}

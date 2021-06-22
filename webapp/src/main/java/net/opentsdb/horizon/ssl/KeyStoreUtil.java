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

import com.google.common.io.Resources;
import com.oath.auth.KeyRefresher;
import com.oath.auth.KeyRefresherException;
import com.oath.auth.Utils;

import javax.net.ssl.KeyManager;
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManager;
import javax.net.ssl.TrustManagerFactory;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.UnrecoverableKeyException;

public class KeyStoreUtil {

  public static final String TLSV1_2 = "TLSv1.2";

  public static KeyStore loadKeyStore(final String jksFilePath, final String password)
      throws Exception {

    final KeyStore keyStore = KeyStore.getInstance("JKS");
    if (new File(jksFilePath).isAbsolute()) {
      try (InputStream jksFileInputStream = new FileInputStream(jksFilePath)) {
        keyStore.load(jksFileInputStream, password.toCharArray());
        return keyStore;
      }
    }

    try (InputStream jksFileInputStream = Resources.getResource(jksFilePath).openStream()) {
      keyStore.load(jksFileInputStream, password.toCharArray());
      return keyStore;
    }
  }

  public static KeyManager[] getKeyManagers(KeyStore keystore, String password)
      throws NoSuchAlgorithmException, KeyStoreException, UnrecoverableKeyException {
    final KeyManagerFactory keyManagerFactory = KeyManagerFactory
        .getInstance(KeyManagerFactory.getDefaultAlgorithm());
    keyManagerFactory.init(keystore, password.toCharArray());
    return keyManagerFactory.getKeyManagers();
  }

  public static TrustManager[] getTrustManagers(final String jksFilePath, final String password)
      throws Exception {
    KeyStore trustStore = loadKeyStore(jksFilePath, password);
    final TrustManagerFactory trustManagerFactory = TrustManagerFactory
        .getInstance(TrustManagerFactory.getDefaultAlgorithm());
    trustManagerFactory.init(trustStore);
    return trustManagerFactory.getTrustManagers();
  }

  public static SSLContext buildSSLContext(KeyManager[] keyManagers, TrustManager[] trustManagers)
      throws Exception {
    final SSLContext sslContext = SSLContext.getInstance(TLSV1_2);
    sslContext.init(keyManagers, trustManagers, null);
    return sslContext;
  }

  /**
   * Builds an auto refreshed {@link SSLContext} with the given certificate and the key.
   *
   * @param key private key
   * @param cert certificate
   * @param trustStore a JKS file containing the trust material
   * @param trustStorePassword password of the <code>trustStore</code>
   * @param refreshIntervalMillis certificate refresh interval in milliseconds
   * @return {@link SSLContext}
   * @throws IOException
   * @throws InterruptedException
   * @throws KeyRefresherException
   */
  public static SSLContext buildSSLContext(
      String key,
      String cert,
      String trustStore,
      String trustStorePassword,
      int refreshIntervalMillis)
      throws IOException, InterruptedException, KeyRefresherException {
    KeyRefresher keyRefresher =
        Utils.generateKeyRefresher(trustStore, trustStorePassword, cert, key);
    keyRefresher.startup(refreshIntervalMillis);
    return Utils.buildSSLContext(
        keyRefresher.getKeyManagerProxy(), keyRefresher.getTrustManagerProxy());
  }
}

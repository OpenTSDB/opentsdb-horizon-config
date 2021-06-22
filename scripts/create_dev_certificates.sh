#!/bin/bash

#
# This file is part of OpenTSDB.
#  Copyright (C) 2021 Yahoo.
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express  implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.
#

############### DON'T RUN THIS ON PROD OR STAGE HOST #####################

# Generates test certificates for mutual TLS authentication testing without real Athenz
# Re-run this if the certs expire

# Assuming you are running from project root cmd: scripts/create_dev_certificates.sh
# setting output to correct resources directory:
D=src/test/resources

# Create test CA
openssl genrsa -out test_rootCA.key 2048
openssl req -x509 -new -nodes -key test_rootCA.key -sha256 -days 1024 -out test_rootCA.crt \
  -subj "/C=US/ST=CA/O=Test CA/CN=testCA"

# Create private keys
openssl genrsa -out test_server.key 2048

# Create certificate requests
openssl req -new -key test_server.key -out /tmp/server.csr -subj "/C=US/ST=CA/O=Opentsdb Test/CN=server"

# Create certificates
openssl x509 -req -in /tmp/server.csr -CA test_rootCA.crt -CAkey test_rootCA.key -CAcreateserial -out test_server.crt -days 1024 -sha256

#Create pkcs12 store with the server key and cert
openssl pkcs12 -export -in test_server.crt -inkey test_server.key -out keystore.p12 -name testServer -CAfile test_rootCA.crt -caname testCA -password pass:changeit

# Create key store and import server key, cert and the ca cert into it
rm javakeystore.jks
echo "yes" | keytool -import -file test_rootCA.crt -alias testCA -keystore webapp/javakeystore.jks -storepass changeit
echo "yes" | keytool -importkeystore -deststorepass changeit -destkeypass changeit -destkeystore webapp/javakeystore.jks -srckeystore keystore.p12 -srcstoretype PKCS12 -srcstorepass changeit

# Remove the temp files - we don't need it anymore
rm test_rootCA.crt
rm test_rootCA.key
rm test_rootCA.srl
rm test_server.key
rm test_server.crt
rm keystore.p12

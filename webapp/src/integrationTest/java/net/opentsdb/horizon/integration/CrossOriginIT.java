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

package net.opentsdb.horizon.integration;

import static com.jayway.restassured.RestAssured.given;
import static org.jboss.resteasy.spi.CorsHeaders.ACCESS_CONTROL_ALLOW_CREDENTIALS;
import static org.jboss.resteasy.spi.CorsHeaders.ACCESS_CONTROL_ALLOW_HEADERS;
import static org.jboss.resteasy.spi.CorsHeaders.ACCESS_CONTROL_ALLOW_METHODS;
import static org.jboss.resteasy.spi.CorsHeaders.ACCESS_CONTROL_ALLOW_ORIGIN;
import static org.jboss.resteasy.spi.CorsHeaders.ACCESS_CONTROL_MAX_AGE;
import static org.jboss.resteasy.spi.CorsHeaders.ACCESS_CONTROL_REQUEST_HEADERS;
import static org.jboss.resteasy.spi.CorsHeaders.ACCESS_CONTROL_REQUEST_METHOD;
import static org.jboss.resteasy.spi.CorsHeaders.ORIGIN;
import static org.junit.jupiter.api.Assertions.assertEquals;

import com.jayway.restassured.http.ContentType;
import com.jayway.restassured.response.Headers;
import com.jayway.restassured.response.Response;
import org.junit.jupiter.api.Test;

public class CrossOriginIT extends BaseIT {

    @Override
    protected String getUri() {
        return "user";
    }

    @Test
    public void testPreflight() {
        String origin = "dev-config.opentsdb.net";
        Response response = given().config(ignoreSslCertificateValidation).contentType(ContentType.JSON)
            .header(ORIGIN, origin).header(ACCESS_CONTROL_REQUEST_METHOD, "POST")
            .header(ACCESS_CONTROL_REQUEST_HEADERS, "Accept, Content-Type")
            .options(endPoint);
        Headers headers = response.getHeaders();

        assertEquals(200, response.getStatusCode(), response.getBody().asString());
        assertEquals(origin, headers.get(ACCESS_CONTROL_ALLOW_ORIGIN).getValue());
        assertEquals("Accept, Content-Type, Origin, X-Requested-With", headers.get(ACCESS_CONTROL_ALLOW_HEADERS).getValue());
        assertEquals("GET, DELETE, OPTIONS, PATCH, POST, PUT", headers.get(ACCESS_CONTROL_ALLOW_METHODS).getValue());
        assertEquals("true", headers.get(ACCESS_CONTROL_ALLOW_CREDENTIALS).getValue());
        assertEquals("1800", headers.get(ACCESS_CONTROL_MAX_AGE).getValue());
    }
}

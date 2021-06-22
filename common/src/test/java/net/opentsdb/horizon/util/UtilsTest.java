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

package net.opentsdb.horizon.util;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.io.FileUtils;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class UtilsTest {

  private ObjectMapper objectMapper = new ObjectMapper();

  @Test
  void testSlugify() {
    assertEquals(
        "214-offline-taxonomy-microsoft-has-failed-jobs",
        Utils.slugify("214-Offline Taxonomy Microsoft has failed jobs"));
  }

  @Test
  void testGZipCompression() throws IOException {
    String file = getClass().getClassLoader().getResource("dashboards/d_large.json").getFile();
    String original = FileUtils.readFileToString(new File(file), "utf8");
    byte[] originalBytes = original.getBytes();
    byte[] compressedBytes = Utils.compress(originalBytes);
    assertTrue(compressedBytes.length < originalBytes.length);

    byte[] decompressedBytes = Utils.decompress(compressedBytes);
    assertEquals(originalBytes.length, decompressedBytes.length);
    assertEquals(original, new String(decompressedBytes));
  }

  @Test
  void testSerialization() throws IOException {

    Map<String, String> content = new HashMap();
    content.put("k1", "v1");
    content.put("k2", "v2");

    String serialized = Utils.serialize(content);

    Object deSerialized = objectMapper.readValue(serialized, Object.class);
    assertTrue(deSerialized instanceof Map);
    assertThat(deSerialized, is(content));
  }

  @Test
  void testDeserialization() throws IOException {
    Map<String, String> content = new HashMap();
    content.put("k1", "v1");
    content.put("k2", "v2");
    byte[] bytes = objectMapper.writeValueAsString(content).getBytes();

    Object deSerialized = Utils.deSerialize(bytes, Object.class);
    assertTrue(deSerialized instanceof Map);
    assertThat(deSerialized, is(content));
  }

}

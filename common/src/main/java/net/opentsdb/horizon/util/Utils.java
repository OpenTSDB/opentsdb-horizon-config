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
import com.github.slugify.Slugify;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Collection;
import java.util.Map;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

public class Utils {

  private static final byte BYTE_1 = 1;
  private static final byte BYTE_0 = 0;

  private static final Slugify slug = new Slugify();
  private static ObjectMapper objectMapper = new ObjectMapper();

  public static boolean isNullOrEmpty(String s) {
    return s == null || s.isEmpty();
  }

  public static boolean isNullOrEmpty(Map<?, ?> map) {
    return map == null || map.isEmpty();
  }

  public static boolean isNullOrEmpty(Collection<?> c) {
    return c == null || c.isEmpty();
  }

  public static void checkArgument(final boolean condition, final String message) {
    if (!condition) {
      throw new IllegalArgumentException(message);
    }
  }

  public static String slugify(String word) {
    return slug.slugify(word);
  }

  public static String serialize(final Object content) throws IOException {
    return objectMapper.writeValueAsString(content);
  }

  public static <T> T deSerialize(final byte[] bytes, Class<T> type) throws IOException {
    return deSerialize(new String(bytes), type);
  }

  public static <T> T deSerialize(final String data, Class<T> type) throws IOException {
    return objectMapper.readValue(data, type);
  }

  public static byte[] compress(final byte[] data) throws IOException {
    try (ByteArrayOutputStream bos = new ByteArrayOutputStream(data.length)) {
      try (GZIPOutputStream gzip = new GZIPOutputStream(bos)) {
        gzip.write(data);
        gzip.flush();
      }
      return bos.toByteArray();
    }
  }

  public static byte[] decompress(final byte[] compressed) throws IOException {
    try (ByteArrayInputStream in = new ByteArrayInputStream(compressed)) {
      try (GZIPInputStream gzip = new GZIPInputStream(in)) {
        try (ByteArrayOutputStream out = new ByteArrayOutputStream()) {
          int length;
          byte[] buffer = new byte[1024];
          while ((length = gzip.read(buffer)) > 0) {
            out.write(buffer, 0, length);
          }
          return out.toByteArray();
        }
      }
    }
  }
}

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

package net.opentsdb.horizon.store;

import java.io.IOException;
import java.sql.SQLException;
import java.util.Objects;

@FunctionalInterface
public interface StoreFunction<T, R> {

  R apply(T t) throws SQLException, IOException;

  default <V> StoreFunction<V, R> compose(StoreFunction<? super V, ? extends T> before) {
    Objects.requireNonNull(before);
    return (V v) -> apply(before.apply(v));
  }

  default <V> StoreFunction<T, V> andThen(StoreFunction<? super R, ? extends V> after) {
    Objects.requireNonNull(after);
    return (T t) -> after.apply(apply(t));
  }

  static <T> StoreFunction<T, T> identity() {
    return t -> t;
  }
}

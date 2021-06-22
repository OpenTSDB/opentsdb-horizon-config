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

package net.opentsdb.horizon.fs;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

public class Path {

  private String path;
  private String parentPath;
  private boolean isRoot;
  private RootType rootType;
  private String rootName;
  private String leaf;

  private static MessageDigest digest;

  static {
    try {
      digest = MessageDigest.getInstance("md5");
    } catch (NoSuchAlgorithmException e) {
      throw new IllegalArgumentException(e);
    }
  }

  public static Path get(String pathString) throws PathException {
    return new Path(pathString);
  }

  public static Path getByUserId(String userId) throws PathException {
    return new Path(RootType.user.formatPathString(userId));
  }

  public static Path getPathByNamespace(String namespace) throws PathException {
    return new Path(RootType.namespace.formatPathString(namespace));
  }

  private Path(String pathString) throws PathException {
    pathString = normalize(pathString);

    String[] split = pathString.split("/");
    if (split.length < 3) {
      throw new PathException("Invalid path " + pathString);
    }

    try {
      this.rootType = RootType.valueOf(split[1]);
    } catch (IllegalArgumentException e) {
      throw new PathException("Invalid path " + pathString);
    }

    this.rootName = split[2];
    this.isRoot = split.length == 3;
    this.leaf = split[split.length - 1];
    this.path = pathString;
    this.parentPath = pathString.substring(0, pathString.lastIndexOf(leaf) - 1);
  }

  public static String normalize(String path) {
    path = path.trim();
    if (!path.startsWith("/")) {
      path = '/' + path;
    }
    if (path.endsWith("/")) {
      path = path.substring(0, path.lastIndexOf('/'));
    }
    return path.toLowerCase();
  }

  public static String getLeaf(String pathString) {
    return pathString.substring(pathString.lastIndexOf('/') + 1);
  }

  public String getPath() {
    return path;
  }

  public boolean isRoot() {
    return isRoot;
  }

  public RootType getRootType() {
    return rootType;
  }

  public String getRootName() {
    return rootName;
  }

  public String getLeaf() {
    return leaf;
  }

  public void stLeaf(String leaf) {
    String parentPath = getParentPath();
    String normalized = normalize(leaf);
    this.leaf = normalized.substring(1);
    this.path = parentPath + normalized;
  }

  public byte[] hash() {
    return hash(path.getBytes());
  }

  public static byte[] hash(String pathString) {
    return hash(pathString.getBytes());
  }

  private static byte[] hash(byte[] bytes) {
    return digest.digest(bytes);
  }

  public String getChildPath(String child) {
    return getChildPath(path, child);
  }

  public static String getChildPath(String parentPath, String child) {
    return parentPath + normalize(child);
  }

  public String getParentPath() {
    return path.substring(0, path.lastIndexOf(leaf) - 1);
  }

  public boolean isAncestor(Path that) {
    return !isSibling(that) && that.parentPath.startsWith(this.parentPath);
  }

  public boolean isSibling(Path that) {
    return this.parentPath.equals(that.parentPath);
  }

  static String userTypePrefix = RootType.user.name() + ".";

  public enum RootType {
    user {
      @Override
      String formatPathString(String userId) {
        userId = userId.replace('.', '/');
        if (!userId.startsWith("user/")) {
          userId = "user/" + userId;
        }
        return userId;
      }
    },
    namespace {
      @Override
      String formatPathString(String namespace) {
        return "/namespace/" + namespace;
      }
    };

    abstract String formatPathString(String name);
  }

  public static String getUserId(String typedUserId) {
    if (typedUserId.startsWith(userTypePrefix)) {
      return typedUserId.substring(typedUserId.indexOf(userTypePrefix) + userTypePrefix.length());
    }
    return typedUserId;
  }

  public static class PathException extends Exception {
    public PathException(String message) {
      super(message);
    }
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (!(obj instanceof Path)) {
      return false;
    }

    Path that = (Path) obj;
    return this.path.equals(that.path);
  }

  @Override
  public int hashCode() {
    return path.hashCode();
  }
}

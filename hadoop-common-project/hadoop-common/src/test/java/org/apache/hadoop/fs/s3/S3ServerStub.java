/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.fs.s3;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.io.FileUtils;
import org.jets3t.service.S3ObjectsChunk;
import org.jets3t.service.S3Service;
import org.jets3t.service.S3ServiceException;
import org.jets3t.service.acl.AccessControlList;
import org.jets3t.service.model.S3Bucket;
import org.jets3t.service.model.S3BucketLoggingStatus;
import org.jets3t.service.model.S3Object;
import org.jets3t.service.model.S3Owner;
import org.jets3t.service.security.AWSCredentials;

/**
 * Stub emulates a s3 server an access to buckets and objects
 * 
 */
public class S3ServerStub extends S3Service {

  private static final long serialVersionUID = 4845644480879625579L;
  private String basePath;
  private final Map<String, AccessControlList> aclStorage = new HashMap<String, AccessControlList>();
  private boolean throwException = false;
  private Map<String, String> metaData = null;

  public void setMetaData(Map<String, String> metaData) {
    this.metaData = metaData;
  }

  public void setThrowException(boolean throwException) {
    this.throwException = throwException;
  }

  protected S3ServerStub(AWSCredentials awsCredentials, String basePath)
      throws S3ServiceException {
    super(awsCredentials);
    this.basePath = basePath;
    if (this.basePath.endsWith("/") || this.basePath.endsWith("\\")) {
      this.basePath += "/";
    }

  }

  @Override
  public boolean isBucketAccessible(String bucketName)
      throws S3ServiceException {
    File f = new File(basePath + bucketName);
    return f.exists() && f.isDirectory();
  }

  @Override
  public int checkBucketStatus(String bucketName) throws S3ServiceException {
    return isBucketAccessible(bucketName) ? BUCKET_STATUS__MY_BUCKET
        : BUCKET_STATUS__DOES_NOT_EXIST;
  }

  @Override
  protected String getBucketLocationImpl(String bucketName)
      throws S3ServiceException {
    File f = new File(basePath + bucketName);
    return f.getAbsolutePath();
  }

  @Override
  protected S3BucketLoggingStatus getBucketLoggingStatusImpl(String bucketName)
      throws S3ServiceException {
    return new S3BucketLoggingStatus(bucketName, bucketName + "prefix");
  }

  @Override
  protected void setBucketLoggingStatusImpl(String bucketName,
      S3BucketLoggingStatus status) throws S3ServiceException {
    File f = new File(basePath + bucketName);
    if (!f.exists()) {
      throw new S3ServiceException("Exception message", getErrorMessage("1","File not exist"));
    }

  }

  @Override
  protected S3Bucket[] listAllBucketsImpl() throws S3ServiceException {
    File f = new File(basePath);
    File[] child = f.listFiles();
    if (child != null) {
      List<S3Bucket> result = new ArrayList<S3Bucket>();
      for (File file : child) {
        if (file.isDirectory()) {
          result.add(new S3Bucket(file.getName()));
        }
      }
      return result.toArray(new S3Bucket[result.size()]);
    }
    return new S3Bucket[0];
  }

  @Override
  protected S3Object[] listObjectsImpl(String bucketName, String prefix,
      String delimiter, long maxListingLength) throws S3ServiceException {
    if (throwException) {
      throw new S3ServiceException("Exception", getErrorMessage("2","List objects exception"));
    }
    if (prefix == null) {
      prefix = "";
    } else if ("%2F".equals(prefix)) {
      prefix = "/";
    }
    File f = new File(basePath + bucketName + prefix);
    File[] child = f.listFiles();
    if (child == null) {
      return new S3Object[0];
    }
    List<S3Object> result = new ArrayList<S3Object>();
    for (File file : child) {
      if (file.isFile()) {
        result
            .add(new S3Object(new S3Bucket(bucketName), "/" + file.getName()));
      }
    }
    return result.toArray(new S3Object[result.size()]);
  }

  @Override
  protected S3ObjectsChunk listObjectsChunkedImpl(String bucketName,
      String prefix, String delimiter, long maxListingLength,
      String priorLastKey, boolean completeListing) throws S3ServiceException {

    File f = new File(basePath + bucketName);
    File[] child = f.listFiles();
    List<S3Object> result = new ArrayList<S3Object>();
    if (child != null) {
      for (File file : child) {
        if (file.isFile()) {
          result.add(new S3Object(new S3Bucket(bucketName), "/"
              + file.getName()));
        }
      }
    }
    S3Object[] objects = result.toArray(new S3Object[result.size()]);
    String[] commonPrefix = new String[objects.length];
    for (int i = 0; i < objects.length; i++) {
      commonPrefix[i] = "";
    }

    return new S3ObjectsChunk(prefix, delimiter, objects, commonPrefix,
        priorLastKey);
  }

  @Override
  protected S3Bucket createBucketImpl(String bucketName, String location,
      AccessControlList acl) throws S3ServiceException {
    File f = new File(basePath + location + File.separator + bucketName);

    S3Bucket result = new S3Bucket();
    if (!f.mkdirs()) {
      throw new S3ServiceException("cannot create directory:"
          + f.getAbsolutePath());
    }
    result.setAcl(acl);
    result.setCreationDate(new Date(System.currentTimeMillis()));
    result.setLocation(location);
    result.setName(bucketName);
    result.setOwner(new S3Owner("user", "User"));
    return result;
  }

  @Override
  protected void deleteBucketImpl(String bucketName) throws S3ServiceException {
    File f = new File(basePath + bucketName);
    if (f.exists() && f.isDirectory()) {
      try {
        FileUtils.deleteDirectory(f);
      } catch (IOException e) {
        throw new S3ServiceException("Exception message", getErrorMessage("6","File system error:"+e.getMessage()));
      }
    }

  }

  @Override
  protected S3Object putObjectImpl(String bucketName, S3Object object)
      throws S3ServiceException {
    if (throwException) {
      throw new S3ServiceException("Exception message", getErrorMessage("4","Put object exception"));

    }
    try {
      File f = new File(basePath + bucketName + File.separator
          + object.getKey());
      if (!f.getParentFile().exists()) {
        if (!f.getParentFile().mkdirs()) {
          throw new S3ServiceException("cannot create new directory:"
              + f.getParentFile());
        }
      }
      if (f.createNewFile()) {
        if (object.getDataInputStream() != null) {
          writeFile(f, object.getDataInputStream());
        }
      }
    } catch (IOException e) {
      throw new S3ServiceException("Exception message", getErrorMessage("6","File system error:"+e.getMessage()));

    }
    return object;
  }

  private void writeFile(File result, InputStream in) throws IOException {

    OutputStream out = new FileOutputStream(result);
    try {
      byte[] buffer = new byte[512];
      int read;
      while ((read = in.read(buffer)) >= 0) {
        out.write(buffer, 0, read);
      }
      out.flush();
    } finally {
      if (out != null) {
        out.close();
      }
    }

  }

  @SuppressWarnings("rawtypes")
  @Override
  protected Map copyObjectImpl(String sourceBucketName, String sourceObjectKey,
      String destinationBucketName, String destinationObjectKey,
      AccessControlList acl, Map destinationMetadata, Calendar ifModifiedSince,
      Calendar ifUnmodifiedSince, String[] ifMatchTags, String[] ifNoneMatchTags)
      throws S3ServiceException {
    File original = new File(basePath + sourceBucketName + File.separator
        + sourceObjectKey);

    File copy = new File(basePath + destinationBucketName + File.separator
        + destinationObjectKey);

    try {
      FileUtils.copyFile(original, copy);
    } catch (IOException e) {
      throw new S3ServiceException("Exception message", getErrorMessage("6","File system error:"+e.getMessage()));
    }

    return new HashMap<Object, Object>();
  }

  @Override
  protected void deleteObjectImpl(String bucketName, String objectKey)
      throws S3ServiceException {
    if (throwException) {
      throw new S3ServiceException("exception", getErrorMessage("7","Delete exception"));
    }
    if (objectKey.startsWith("%2F")) {
      objectKey = objectKey.substring(3);
    }
    File f = new File(basePath + bucketName + File.separator + objectKey);
    if (!f.delete()) {
      throw new S3ServiceException("Exception message", getErrorMessage("10","Try to delete non exist node"));
    }

  }

  @Override
  protected S3Object getObjectDetailsImpl(String bucketName, String objectKey,
      Calendar ifModifiedSince, Calendar ifUnmodifiedSince,
      String[] ifMatchTags, String[] ifNoneMatchTags) throws S3ServiceException {
    File f = new File(basePath + bucketName + File.separator + objectKey);
    S3Bucket bucket = new S3Bucket(bucketName, f.getParent());
    return new S3Object(bucket, objectKey);
  }

  @Override
  protected S3Object getObjectImpl(String bucketName, String objectKey,
      Calendar ifModifiedSince, Calendar ifUnmodifiedSince,
      String[] ifMatchTags, String[] ifNoneMatchTags, Long byteRangeStart,
      Long byteRangeEnd) throws S3ServiceException {
    if (throwException) {
      throw new S3ServiceException("exception", getErrorMessage("8","get object exception"));
    }
    if (objectKey.startsWith("%2F")) { // first /
      objectKey = objectKey.substring(3);
    }
    File f = new File(basePath + bucketName + File.separator + objectKey);
    S3Bucket bucket = new S3Bucket(bucketName, f.getParent());
    S3Object result = new S3Object(bucket, objectKey);
    if (metaData == null) {
      result.addMetadata("fs", "Hadoop");
      result.addMetadata("fs-type", "block");
      result.addMetadata("fs-version", "1");
    } else {
      result.addAllMetadata(metaData);
    }
    if (f.exists()) {
      result.setDataInputFile(f);
    }
    return result;
  }

  @Override
  protected void putBucketAclImpl(String bucketName, AccessControlList acl)
      throws S3ServiceException {

    aclStorage.put(bucketName, acl);
  }

  @Override
  protected void putObjectAclImpl(String bucketName, String objectKey,
      AccessControlList acl) throws S3ServiceException {
    aclStorage.put(bucketName + File.separator + objectKey, acl);

  }

  @Override
  protected AccessControlList getObjectAclImpl(String bucketName,
      String objectKey) throws S3ServiceException {

    return aclStorage.get(bucketName + File.separator + objectKey);
  }

  @Override
  protected AccessControlList getBucketAclImpl(String bucketName)
      throws S3ServiceException {

    return aclStorage.get(bucketName);
  }

  private String getErrorMessage(String code,String message) {
    return "<Error><Code>"+code+"</Code><Message>"+message+"</Message><RequestId>23456</RequestId><HostId>localhost</HostId></Error>";
  }
}

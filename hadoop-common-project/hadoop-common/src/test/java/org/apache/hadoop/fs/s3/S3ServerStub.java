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

public class S3ServerStub extends S3Service {

  private static final long serialVersionUID = 4845644480879625579L;
  private String basePath;
  private final Map<String, AccessControlList> aclStorage = new HashMap<String, AccessControlList>();
  private boolean throwException=false;
  
  
 

  public boolean isThrowException() {
    return throwException;
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
    if (!f.exists()) {
      return false;
    }
    return f.isDirectory();
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
    S3BucketLoggingStatus result = new S3BucketLoggingStatus(bucketName,
        bucketName + "prefix");
    return result;
  }

  @Override
  protected void setBucketLoggingStatusImpl(String bucketName,
      S3BucketLoggingStatus status) throws S3ServiceException {
    File f = new File(basePath + bucketName);
    if (!f.exists()) {
      throw new S3ServiceException("Exception message", getErrorMessage());
    }

  }

  @Override
  protected S3Bucket[] listAllBucketsImpl() throws S3ServiceException {
    File f = new File(basePath);
    File[] child = f.listFiles();
    List<S3Bucket> result = new ArrayList<S3Bucket>();
    for (File file : child) {
      if (file.isDirectory()) {
        result.add(new S3Bucket(file.getName()));
      }
    }
    return result.toArray(new S3Bucket[result.size()]);
  }

  @Override
  protected S3Object[] listObjectsImpl(String bucketName, String prefix,
      String delimiter, long maxListingLength) throws S3ServiceException {
    File f = new File(basePath+bucketName);
    File[] child = f.listFiles();
    List<S3Object> result = new ArrayList<S3Object>();
    for (File file : child) {
      if (file.isFile()) {
        result.add(new S3Object(new S3Bucket(bucketName), "/"+file.getName()));
      }
    }
    return result.toArray(new S3Object[result.size()]);
  }

  @Override
  protected S3ObjectsChunk listObjectsChunkedImpl(String bucketName,
      String prefix, String delimiter, long maxListingLength,
      String priorLastKey, boolean completeListing) throws S3ServiceException {
    
    
    File f = new File(basePath+bucketName);
    File[] child = f.listFiles();
    List<S3Object> result = new ArrayList<S3Object>();
    for (File file : child) {
      if (file.isFile()) {
        result.add(new S3Object(new S3Bucket(bucketName), "/"+file.getName()));
      }
    }
    S3Object[] objects= result.toArray(new S3Object[result.size()]);
    String[] commonPrefix= new String[objects.length];
    for (int i=0; i<objects.length ; i++) {
      commonPrefix[i]="";
    }
    
    return new S3ObjectsChunk(prefix,delimiter,objects,commonPrefix,priorLastKey);
  }

  @Override
  protected S3Bucket createBucketImpl(String bucketName, String location,
      AccessControlList acl) throws S3ServiceException {
    File f = new File(basePath + location + File.separator + bucketName);
    f.mkdirs();
    S3Bucket result = new S3Bucket();
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
        return;
      } catch (IOException e) {
        throw new S3ServiceException("Exception message", getErrorMessage());
      }
    }
    throw new S3ServiceException("Exception message", getErrorMessage());

  }

  @Override
  protected S3Object putObjectImpl(String bucketName, S3Object object)
      throws S3ServiceException {
    try {
      File f = new File(basePath + bucketName);
      if (!f.exists()) {
        f.mkdirs();
      }
      f = new File(basePath + bucketName + File.separator + object.getKey());
      f.createNewFile();
      if (object.getDataInputStream() != null) {
        writeFile(f, object.getDataInputStream());
      }
    } catch (Throwable e) {
      throw new S3ServiceException("Exception message", getErrorMessage());

    }
    return object;
  }

  private void writeFile(File result, InputStream in) throws Exception {

    OutputStream out = new FileOutputStream(result);
    byte[] buffer = new byte[512];
    int read = 0;
    while ((read = in.read(buffer)) >= 0) {
      out.write(buffer, 0, read);
    }

    out.flush();
    out.close();

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
      throw new S3ServiceException("Exception message", getErrorMessage());
    }

    return new HashMap<Object, Object>();
  }

  @Override
  protected void deleteObjectImpl(String bucketName, String objectKey)
      throws S3ServiceException {
    File f = new File(basePath + bucketName + File.separator + objectKey);
    if (!f.delete()) {
      throw new S3ServiceException("Exception message", getErrorMessage());
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
    if(throwException){
      throw new S3ServiceException("exception",getErrorMessage());
    }
    File f = new File(basePath + bucketName + File.separator + objectKey);
    S3Bucket bucket = new S3Bucket(bucketName, f.getParent());
    S3Object result = new S3Object(bucket, objectKey);
    result.addMetadata("fs", "Hadoop");
    result.addMetadata("fs-type", "block");
    result.addMetadata("fs-version", "1");
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

  private String getErrorMessage() {
    String result = "<Error>" + "<Code>12345</Code>" + "<Message>msg</Message>"
        + "<RequestId>23456</RequestId>" + "<HostId>host</HostId>" + "</Error>";
    return result;
  }
}

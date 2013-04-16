package org.apache.hadoop.test;

import java.io.File;

import org.apache.hadoop.fs.Path;

public class PathUtils {

  public static Path getTestPath(Class<?> caller) {
    return getTestPath(caller, true);
  }

  public static Path getTestPath(Class<?> caller, boolean create) {
    return new Path(getTestDirName(caller));
  }

  public static File getTestDir(Class<?> caller) {
    return getTestDir(caller, true);
  }
  
  public static File getTestDir(Class<?> caller, boolean create) {
    File dir = new File(System.getProperty("test.build.data", "/tmp"), caller.getSimpleName());
    if (create) {
      dir.mkdirs();
    }
    return dir;
  }

  public static String getTestDirName(Class<?> caller) {
    return getTestDirName(caller, true);
  }
  
  public static String getTestDirName(Class<?> caller, boolean create) {
    return getTestDir(caller, create).getAbsolutePath();
  }
    
}

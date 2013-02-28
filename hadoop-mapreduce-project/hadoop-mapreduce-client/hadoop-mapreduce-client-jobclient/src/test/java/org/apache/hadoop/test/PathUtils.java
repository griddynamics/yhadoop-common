package org.apache.hadoop.test;

import java.io.File;

import org.apache.hadoop.fs.Path;

public class PathUtils {

  public static Path getTestPath(Class<?> caller) {
    return new Path(getTestDirName(caller));
  }

  public static File getTestDir(Class<?> caller) {
    return new File(getTestDirName(caller));
  }

  public static String getTestDirName(Class<?> caller) {
    String dir = System.getProperty("test.build.data", "/tmp") + File.separator + caller.getSimpleName();
    dir = dir.replace(' ', '+');
    return dir;
  }
    
}

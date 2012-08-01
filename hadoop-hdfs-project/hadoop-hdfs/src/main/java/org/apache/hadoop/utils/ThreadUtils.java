package org.apache.hadoop.utils;


public class ThreadUtils {

    private static int coreSpeedFactor = 10;
    private static int testSpeedFactor = 10;

    public static void sleep(long t) throws InterruptedException {
        long delay = t;
        if(t > 100) {
            if(getCallerClassName(2).matches(".*Test.*")) {
                delay /= testSpeedFactor;
            } else {
                delay /= coreSpeedFactor;
            }
        }
        Thread.sleep(delay);
    }

    public static String getCallerClassName(int depth) {
        return new Throwable().fillInStackTrace().getStackTrace()[depth].getClassName();
    }

}

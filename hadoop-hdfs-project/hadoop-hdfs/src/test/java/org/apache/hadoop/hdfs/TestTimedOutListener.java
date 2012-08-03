package org.apache.hadoop.hdfs;

import java.util.Map;

import org.junit.runner.notification.Failure;
import org.junit.runner.notification.RunListener;

/**
 * JUnit run listener which prints full thread dump into System.err
 * in case a test is failed due to timeout.
 */
public class TestTimedOutListener extends RunListener {

  @Override
  public void testFailure(Failure failure) throws Exception {
    System.err.println("Got failure '" + failure.getMessage() + "'");
    if (failure.getMessage().startsWith("test timed out after")) {
      System.err.println(buildThreadDump());
    }
  }

  private String buildThreadDump() {
    StringBuilder dump = new StringBuilder();
    for (Map.Entry<Thread, StackTraceElement[]> e : Thread.getAllStackTraces().entrySet()) {
      Thread thread = e.getKey();
      dump.append(String.format("\"%s\" %s prio=%d tid=%d %s\njava.lang.Thread.State: %s",
        thread.getName(),
        (thread.isDaemon() ? "daemon" : ""),
        thread.getPriority(),
        thread.getId(),
        Thread.State.WAITING.equals(thread.getState()) ? "in Object.wait()" : thread.getState().name().toLowerCase(),
        (thread.getState().equals(Thread.State.WAITING) ? "WAITING (on object monitor)" : thread.getState())));
      for (StackTraceElement stackTraceElement : e.getValue()) {
        dump.append("\n        at ");
        dump.append(stackTraceElement);
      }
      dump.append("\n");
    }
    return dump.toString();
  }
  
}

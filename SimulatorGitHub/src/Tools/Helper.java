package Tools;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import Model.Configuration;

/**
 * Helper class - helps to simulate time delays
 * @author ibra
 */
public class Helper {

  private static final Log LOG = LogFactory.getLog(Helper.class.getName());

  /**
   * Just sleep
   * @param ms amount of milliseconds to sleep
   */
  private static void sleep(final long ms) {
    try {
      Thread.sleep(ms);
    } catch (final InterruptedException e) {
      e.printStackTrace();
    }
  }

  /**
   * Just sleep 1 millisecond
   */
  public static void sleep() {
    sleep(1);
  }

  /**
   * Sleep till time(ms)
   * @param time time(ms) till what this method sleeps
   */
  public static void sleepTo(final long time) {
    final long d = time - System.currentTimeMillis();
    if (d > 0) {
      sleep(d);
    }
  }

  public static void logSleep() {
    sleep(Configuration.INSTANCE.SLEEP_LOG_TIME);
  }

  public static void chartSleep() {
    sleep(Configuration.INSTANCE.CHARTS_UPDATE_GAP);
  }
}

package Tools;

import java.util.Random;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import Model.Configuration;

/**
 * RandomGenerator - static class, used for getting randomized values
 * @author ibra
 */
public class RandomGenerator {

  private static final Log LOG = LogFactory.getLog(RandomGenerator.class.getName());

  private static final Random rand = new Random(System.currentTimeMillis());

  /**
   * @return random column family
   */
  public static int getNextColumnFamily() {
    return getRandomInt((int) Configuration.INSTANCE.getCompactionAlgosCount());
  }

  /**
   * @param conf
   * @return size of new KeyValuePairs pack
   */
  public static long getKeyValuePackBytesSize() {
    return Configuration.INSTANCE.getKvsPerPut() * Configuration.INSTANCE.getKeyValueByteSize();
  }

  /**
   * @return keyValue TTL - random value in range [TTL-TTL*jitter, TTL+TTL*jitter]
   */
  public static long getKeyValueTTL() {
    return getRandomJitteredValue(Configuration.INSTANCE.getKeyValueTTL(),
      Configuration.INSTANCE.getKeyValueTTLJitter());
  }

  /**
   * @param majorCompactionGap base value of major compaction gap
   * @param majorCompactionJitter jitter value of major compaction gap
   * @return majorCompactionGap - random value in range [majorCompactionGap-majorCompactionGap*jitter, 
   *         majorCompactionGap+majorCompactionGap*jitter]
   */
  public static long getMajorCompactionGap(final long majorCompactionGap,
      final double majorCompactionJitter) {
    return getRandomJitteredValue(majorCompactionGap, majorCompactionJitter)
        / Configuration.INSTANCE.getxFaster();
  }

  /**
   * @return random value in range [base-base*jitter, base+base*jitter]
   */
  private static long getRandomJitteredValue(final long base, final double jitter) {
    if (getRandomBoolean()) {
      return (long) (base * (1.0 + getRandomDouble(jitter)));
    } else {
      return (long) (base * (1.0 - getRandomDouble(jitter)));
    }
  }

  /**
   * @param n - upper bound
   * @return random integer in range [0, n)
   */
  public static int getRandomInt(final int n) {
    return rand.nextInt(n);
  }

  /**
   * @param bound - bound
   * @return random double in range [0, n)
   */
  public static double getRandomDouble(final double bound) {
    return rand.nextDouble() * bound;
  }

  /**
   * @return random boolean value
   */
  private static boolean getRandomBoolean() {
    return rand.nextBoolean();
  }

  /**
   * @return random long value
   */
  private static long getRandomLong(final long bound) {
    return (long) getRandomDouble(bound);
  }

  /**
   * @param lo lower bound
   * @param hi upper bound
   * @return random long in range [lo, hi]
   */
  public static long getRandomLong(final long lo, final long hi) {
    return lo + RandomGenerator.getRandomLong(hi - lo + 1);
  }

  /**
   * @param lo lower bound
   * @param hi upper bound
   * @return random double in range [lo, hi]
   */
  public static double getRandomDouble(final double lo, final double hi) {
    return lo + RandomGenerator.getRandomDouble(hi - lo);
  }
}
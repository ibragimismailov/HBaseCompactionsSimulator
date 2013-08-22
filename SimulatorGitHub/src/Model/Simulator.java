package Model;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import GUI.Charts.AdvancedTimeChart;
import GUI.Charts.SimpleTimeChart;
import Model.Compactors.CompactionConfigurations.AbstractCompactionConfiguration;
import Model.HBaseElements.Region;
import Tools.Helper;
import Tools.RandomGenerator;

/**
 * Simulator - simulates HBase. Simulator is enum-based Singleton 
 * - Adds data to HBase with some rate (set in configuration) 
 * - plots graphics about write amplification, read amplification 
 * - provides way to choose different compaction algorithms and different configurations for each
 * store to analyze results separately
 * @author ibra
 */
public enum Simulator {
  INSTANCE;

  private static final Log LOG = LogFactory.getLog(Simulator.class.getName());

  /**
   * simulator simulates HBase work by working with one Region with multiple Stores
   */
  private Region region;

  /**
   * compaction configurations for each store in region. AbstractCompactionConfiguration knows what compaction algorithm to use and contains configuration for it
   */
  private List<AbstractCompactionConfiguration> compactorsConfigurations;

  /**
   * flag for stopping simulator. Should be set only from ConfigurationFrame
   */
  private final AtomicBoolean stop = new AtomicBoolean(false);

  /**
   * Write amplification graph (WAF) - a significant factor in write performance WAF = bytes written
   * to disk during compactions / bytes written to disk during flushes
   */
  private SimpleTimeChart writeAmplificationsTimeChart;

  /**
   * Read amplification graph (RAF) - a significant factor in read performance WAF = amount of
   * storeFiles RAF is number of files you need to check to read some KeyValue (worst case is just
   * amount of StoreFiles)
   */
  private AdvancedTimeChart readAmplificationTimeChart;

  /**
   * this list contains sum of bytes, that were written to disk during flushes for each Store
   */
  private List<Long> flushWrites;

  /**
   * this list contains sum of bytes, that were written to disk during compactions for each Store
   */
  private List<Long> compactionWrites;

  /**
   * initializes and starts simulator with some configurations
   * @param compactorsConfigurations - compaction configurations list (for each store)
   */
  public void start(final List<AbstractCompactionConfiguration> compactorsConfigurations) {
    this.compactorsConfigurations = compactorsConfigurations;
    this.region = new Region((int) Configuration.INSTANCE.getCompactionAlgosCount());
    this.readAmplificationTimeChart = AdvancedTimeChart.go("Stores read amplification", "Time",
      "Stores read amplification", this.region.getStoreTitles());
    this.writeAmplificationsTimeChart = SimpleTimeChart.go("Stores write amplification", "Time",
      "Write amplification", this.region.getStoreTitles());

    this.flushWrites = new ArrayList<Long>();
    this.compactionWrites = new ArrayList<Long>();

    for (int i = 0; i < compactorsConfigurations.size(); i++) {
      this.flushWrites.add(0L);
      this.compactionWrites.add(0L);
    }

    /**
     * separate thread to plot charts
     */
    new Thread(new Runnable() {
      @Override
      public void run() {
        while (!Simulator.this.isStopped()) {
          Simulator.this.writeAmplificationsTimeChart.update(System.currentTimeMillis(),
            Simulator.divide(Simulator.this.compactionWrites, Simulator.this.flushWrites));
          Simulator.this.readAmplificationTimeChart.update(System.currentTimeMillis(),
            Simulator.this.region.getReadAmplification());

          Helper.chartSleep();
        }
      }
    }).start();

    /**
     * separate thread to add data to HBase
     */
    long deadline = System.currentTimeMillis();
    long add = 0;
    while (!Simulator.this.isStopped()) {
      this.region.put(RandomGenerator.getNextColumnFamily());

      final long waitTime = (add + Configuration.INSTANCE.getFlushGap()
          * Configuration.INSTANCE.getKvsPerPut() * Configuration.INSTANCE.getKeyValueByteSize())
          / (Configuration.INSTANCE.getxFaster() * Configuration.INSTANCE.getCompactionAlgosCount() * Configuration.INSTANCE
              .getMemstoreBytesSize());
      add = (add + Configuration.INSTANCE.getFlushGap() * Configuration.INSTANCE.getKvsPerPut()
          * Configuration.INSTANCE.getKeyValueByteSize())
          % (Configuration.INSTANCE.getxFaster() * Configuration.INSTANCE.getCompactionAlgosCount() * Configuration.INSTANCE
              .getMemstoreBytesSize());

      deadline += waitTime;
      Helper.sleepTo(deadline);

      final long d = System.currentTimeMillis() - deadline;
      // random is used not to log too much if we already overslept
      if (d > Math.max(100, waitTime) && RandomGenerator.getRandomInt(250) == 0) {
        LOG.info("OVERSLEPT = " + d);
      }
    }
  }

  /**
   * @param columnFamily - column family of store
   * @return compaction info and configuration for each store
   */
  public AbstractCompactionConfiguration getCompactionConfiguration(final int columnFamily) {
    return this.compactorsConfigurations.get(columnFamily);
  }

  /**
   * @param list1
   * @param list2
   * @return list that contains division of values in lists as list1Value/list2Value
   */
  private static List<Double> divide(final List<Long> list1, final List<Long> list2) {
    final List<Double> res = new ArrayList<Double>();
    for (int i = 0; i < list1.size(); i++) {
      res.add((double) (list1.get(i)) / list2.get(i));
    }
    return res;
  }

  /**
   * event - flush occurred in some store
   * @param columnFamily - columnFamily of store where flush occurred
   * @param byteSize size of bytes written to disk during this flush
   */
  synchronized public void flushOccurred(final int columnFamily, final long byteSize) {
    this.flushWrites.set(columnFamily, this.flushWrites.get(columnFamily) + byteSize);
  }

  /**
   * event - compaction occurred in some store
   * @param columnFamily - columnFamily of store where compaction occurred
   * @param byteSize size of bytes written to disk during this compaction
   */
  synchronized public void compactionOccurred(final int columnFamily, final long byteSize) {
    this.compactionWrites.set(columnFamily, this.compactionWrites.get(columnFamily) + byteSize);
  }

  /**
   * @return if simulator is stopped
   */
  public boolean isStopped() {
    return this.stop.get();
  }

  /**
   * stop simulator
   */
  public void stop() {
    this.stop.set(true);
  }
}
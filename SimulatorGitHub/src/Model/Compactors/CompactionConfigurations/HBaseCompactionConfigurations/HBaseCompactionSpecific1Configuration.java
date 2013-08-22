package Model.Compactors.CompactionConfigurations.HBaseCompactionConfigurations;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import Model.Configuration;

/**
 * HBase compaction algorithm Specific1 configurations
 * @author ibra
 */
public final class HBaseCompactionSpecific1Configuration extends HBaseCompactionConfiguration {

  private static final Log LOG = LogFactory.getLog(HBaseCompactionSpecific1Configuration.class
      .getName());

  /**
   * creates and initializes HBaseCompactionSpecific1Configuration object
   */
  public HBaseCompactionSpecific1Configuration() {
    // init with Specific1 configuration
    this.setMajorCompactionsGap(Long.toString(40L * Configuration.MS_PER_DAY));
    this.setMajorCompactionsJitter(Double.toString(0.4));

    this.setCompactionMinFiles(Long.toString(5L));
    this.setCompactionMaxFiles(Long.toString(10L));
    this.setCompactionMinBytes(Long.toString(0L));
    this.setCompactionMaxBytes(Long.toString(Long.MAX_VALUE));

    this.setCompactionRatio(Double.toString(1.1));
    this.setThrottle(Long.toString(1700000000L));
  }
}

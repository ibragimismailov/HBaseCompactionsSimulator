package Model.Compactors.CompactionConfigurations.HBaseCompactionConfigurations;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import Model.Configuration;

/**
 * HBase compaction algorithm Specific2 configurations
 * @author ibra
 */
public final class HBaseCompactionSpecific2Configuration extends HBaseCompactionConfiguration {

  private static final Log LOG = LogFactory.getLog(HBaseCompactionSpecific2Configuration.class.getName());

  /**
   * creates and initializes HBaseCompactionSpecific2Configuration object
   */
  public HBaseCompactionSpecific2Configuration() {
    // init with Specific2 configuration
    this.setMajorCompactionsGap(Long.toString(20L * Configuration.MS_PER_DAY));
    this.setMajorCompactionsJitter(Double.toString(0.25));

    this.setCompactionMinFiles(Long.toString(8L));
    this.setCompactionMaxFiles(Long.toString(18L));
    this.setCompactionMinBytes(Long.toString(0L));
    this.setCompactionMaxBytes(Long.toString(Long.MAX_VALUE));

    this.setCompactionRatio(Double.toString(1.4));
    this.setThrottle(Long.toString(15000000000L));
  }
}

package Model.Compactors.CompactionConfigurations.HBaseCompactionConfigurations;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import Model.Configuration;

public final class DefaultHBaseCompactionConfiguration extends HBaseCompactionConfiguration {

  private static final Log LOG = LogFactory.getLog(DefaultHBaseCompactionConfiguration.class
      .getName());

  public DefaultHBaseCompactionConfiguration() {
    this.setMajorCompactionsGap(Long.toString(8400000L));
    this.setMajorCompactionsJitter(Double.toString(0.21));

    this.setCompactionMinFiles(Long.toString(2));
    this.setCompactionMaxFiles(Long.toString(12));
    this.setCompactionMinBytes(Long.toString(0));
    this.setCompactionMaxBytes(Long.toString(Long.MAX_VALUE));

    this.setCompactionRatio(Double.toString(1.3));
    this.setThrottle(Long.toString(2 * this.getCompactionMaxFiles()
        * Configuration.INSTANCE.getMemstoreBytesSize()));

    this.setTitle("DefaultHBaseCompactionConfiguration");
  }
}

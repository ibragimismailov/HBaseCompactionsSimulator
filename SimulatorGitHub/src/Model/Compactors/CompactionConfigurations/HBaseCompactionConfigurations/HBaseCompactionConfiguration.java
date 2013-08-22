package Model.Compactors.CompactionConfigurations.HBaseCompactionConfigurations;

import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import Model.Configuration;
import Model.Compactors.HBaseCompactor;
import Model.Compactors.CompactionConfigurations.AbstractCompactionConfiguration;
import Model.HBaseElements.Store;
import Tools.HDFS;

/**
 * native compaction algorithm configuration used in HBase
 * @author ibra
 */
public abstract class HBaseCompactionConfiguration extends AbstractCompactionConfiguration {

  private static final Log LOG = LogFactory.getLog(HBaseCompactionConfiguration.class.getName());

  /**
   * @param hdfs HDFS object - represents HDFS - to read and write with several threads. 
   *        There is one HDFS object per Store
   * @param store - Store to initialize Compactor - store where this Compactor will work
   * @return corresponding Compactor for this CompactionConfiguration
   */
  @Override
  public HBaseCompactor getCompactor(final HDFS hdfs, final Store store) {
    return new HBaseCompactor(hdfs, this, store);
  }

  private long majorCompactionsGap = 86400000L; // hbase.hregion.majorcompaction
  private double majorCompactionsJitter = 0.2; // hbase.hregion.majorcompaction.jitter

  private long compactionMinFiles = 3; // hbase.hstore.compaction.min
  private long compactionMaxFiles = 10; // hbase.hstore.compaction.max
  private long compactionMinBytes = 0L; // hbase.hstore.compaction.min.size
  private long compactionMaxBytes = Long.MAX_VALUE; // hbase.hstore.compaction.max.size

  private double compactionRatio = 1.2; // hbase.store.compaction.ratio

  private long throttle = 2 * this.compactionMaxFiles
      * Configuration.INSTANCE.getMemstoreBytesSize();

  /**
   * fill setFields map
   */
  @Override
  protected Map<String, SetMethod> fillSetFields() {// @formatter:off
    final Map<String, SetMethod> res = super.fillSetFields();
    res.put("Major compactions gap(ms)",  new SetMethod() { @Override public void set(final String value) {HBaseCompactionConfiguration.this.setMajorCompactionsGap   (value);} });
    res.put("Major compactions jitter",   new SetMethod() { @Override public void set(final String value) {HBaseCompactionConfiguration.this.setMajorCompactionsJitter(value);} });
    res.put("Compaction min size(files)", new SetMethod() { @Override public void set(final String value) {HBaseCompactionConfiguration.this.setCompactionMinFiles    (value);} });
    res.put("Compaction max size(files)", new SetMethod() { @Override public void set(final String value) {HBaseCompactionConfiguration.this.setCompactionMaxFiles    (value);} });
    res.put("Compaction min size(bytes)", new SetMethod() { @Override public void set(final String value) {HBaseCompactionConfiguration.this.setCompactionMinBytes    (value);} });
    res.put("Compaction max size(bytes)", new SetMethod() { @Override public void set(final String value) {HBaseCompactionConfiguration.this.setCompactionMaxBytes    (value);} });
    res.put("Compaction ratio",           new SetMethod() { @Override public void set(final String value) {HBaseCompactionConfiguration.this.setCompactionRatio       (value);} });
    res.put("Throttle",                   new SetMethod() { @Override public void set(final String value) {HBaseCompactionConfiguration.this.setThrottle              (value);} });
    return res;
  }// @formatter:on

  /**
   * fill getFields map
   */
  @Override
  protected Map<String, GetMethod> fillGetFields() {// @formatter:off
    final Map<String, GetMethod> res = super.fillGetFields();
    res.put("Major compactions gap(ms)",  new GetMethod() { @Override public String get() {return Long  .toString(HBaseCompactionConfiguration.this.getMajorCompactionsGap   ());} });
    res.put("Major compactions jitter",   new GetMethod() { @Override public String get() {return Double.toString(HBaseCompactionConfiguration.this.getMajorCompactionsJitter());} });
    res.put("Compaction min size(files)", new GetMethod() { @Override public String get() {return Long  .toString(HBaseCompactionConfiguration.this.getCompactionMinFiles    ());} });
    res.put("Compaction max size(files)", new GetMethod() { @Override public String get() {return Long  .toString(HBaseCompactionConfiguration.this.getCompactionMaxFiles    ());} });
    res.put("Compaction min size(bytes)", new GetMethod() { @Override public String get() {return Long  .toString(HBaseCompactionConfiguration.this.getCompactionMinBytes    ());} });
    res.put("Compaction max size(bytes)", new GetMethod() { @Override public String get() {return Long  .toString(HBaseCompactionConfiguration.this.getCompactionMaxBytes    ());} });
    res.put("Compaction ratio",           new GetMethod() { @Override public String get() {return Double.toString(HBaseCompactionConfiguration.this.getCompactionRatio       ());} });
    res.put("Throttle",                   new GetMethod() { @Override public String get() {return Long  .toString(HBaseCompactionConfiguration.this.getThrottle              ());} });
    return res;
  };// @formatter:on

  // ===================================================================
  // =============================<GETTERS>=============================
  // ===================================================================
  public long getMajorCompactionsGap() {
    return this.majorCompactionsGap;
  }

  public double getMajorCompactionsJitter() {
    return this.majorCompactionsJitter;
  }

  public long getCompactionMinFiles() {
    return this.compactionMinFiles;
  }

  public long getCompactionMaxFiles() {
    return this.compactionMaxFiles;
  }

  public long getCompactionMinBytes() {
    return this.compactionMinBytes;
  }

  public long getCompactionMaxBytes() {
    return this.compactionMaxBytes;
  }

  public double getCompactionRatio() {
    return this.compactionRatio;
  }

  public long getThrottle() {
    return this.throttle;
  }

  // ===================================================================
  // ============================</GETTERS>=============================
  // ===================================================================

  // ===================================================================
  // =============================<SETTERS>=============================
  // ===================================================================
  protected void setMajorCompactionsGap(final String majorCompactionsGap) {
    this.majorCompactionsGap = Long.parseLong(majorCompactionsGap);
  }

  protected void setMajorCompactionsJitter(final String majorCompactionsJitter) {
    this.majorCompactionsJitter = Double.parseDouble(majorCompactionsJitter);
  }

  protected void setCompactionMinFiles(final String compactionMinFiles) {
    this.compactionMinFiles = Long.parseLong(compactionMinFiles);
  }

  protected void setCompactionMaxFiles(final String compactionMaxFiles) {
    this.compactionMaxFiles = Long.parseLong(compactionMaxFiles);
  }

  protected void setCompactionMinBytes(final String compactionMinBytes) {
    this.compactionMinBytes = Long.parseLong(compactionMinBytes);
  }

  protected void setCompactionMaxBytes(final String compactionMaxBytes) {
    this.compactionMaxBytes = Long.parseLong(compactionMaxBytes);
  }

  protected void setCompactionRatio(final String compactionRatio) {
    this.compactionRatio = Double.parseDouble(compactionRatio);
  }

  protected void setThrottle(final String throttle) {
    this.throttle = Long.parseLong(throttle);
  }
  // ===================================================================
  // ============================</SETTERS>=============================
  // ===================================================================
}
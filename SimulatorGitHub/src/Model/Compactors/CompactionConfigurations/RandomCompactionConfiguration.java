package Model.Compactors.CompactionConfigurations;

import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import Model.Compactors.RandomCompactor;
import Model.HBaseElements.Store;
import Tools.HDFS;

/**
 * very simple random compaction algorithm configuration
 * @author ibra
 */
public final class RandomCompactionConfiguration extends AbstractCompactionConfiguration {

  private static final Log LOG = LogFactory.getLog(RandomCompactionConfiguration.class.getName());

  /**
   * @param hdfs HDFS object - represents HDFS - to read and write with several threads. 
   *        There is one HDFS object per Store
   * @param store - Store to initialize Compactor - store where this Compactor will work
   * @return corresponding Compactor for this CompactionConfiguration
   */
  @Override
  public RandomCompactor getCompactor(final HDFS hdfs, final Store store) {
    return new RandomCompactor(hdfs, this, store);
  }

  /**
   * if amount of files in storeFiles > than this value we do compaction
   */
  private long maxFiles = 10;

  /**
   * when we do compaction, we select filesToCompact files randomly to compact
   */
  private long filesToCompact = 5;

  /**
   * fill getFields map
   */
  @Override
  protected Map<String, GetMethod> fillGetFields() {// @formatter:off
    final Map<String, GetMethod> res = super.fillGetFields();
    res.put("Max files",        new GetMethod() { @Override public String get() {return Long.toString(RandomCompactionConfiguration.this.getMaxFiles      ());} });
    res.put("Files to compact", new GetMethod() { @Override public String get() {return Long.toString(RandomCompactionConfiguration.this.getFilesToCompact());} });
    return res;
  };// @formatter:on

  /**
   * fill setFields map
   */
  @Override
  protected Map<String, SetMethod> fillSetFields() {// @formatter:off
    final Map<String, SetMethod> res = super.fillSetFields();
    res.put("Max files",        new SetMethod() { @Override public void set(final String value) {RandomCompactionConfiguration.this.setMaxFiles      (value);} });
    res.put("Files to compact", new SetMethod() { @Override public void set(final String value) {RandomCompactionConfiguration.this.setFilesToCompact(value);} });
    return res;
  }// @formatter:on

  // ===================================================================
  // =============================<GETTERS>=============================
  // ===================================================================
  public long getMaxFiles() {
    return this.maxFiles;
  }

  public long getFilesToCompact() {
    return this.filesToCompact;
  }

  // ===================================================================
  // ============================</GETTERS>=============================
  // ===================================================================

  // ===================================================================
  // =============================<SETTERS>=============================
  // ===================================================================
  private void setMaxFiles(final String maxFiles) {
    this.maxFiles = Long.parseLong(maxFiles);
  }

  private void setFilesToCompact(final String filesToCompact) {
    this.filesToCompact = Long.parseLong(filesToCompact);
  }
  // ===================================================================
  // ============================</SETTERS>=============================
  // ===================================================================
}
package Model.Compactors.CompactionConfigurations;

import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import Model.Compactors.IbraCompactor;
import Model.HBaseElements.Store;
import Tools.HDFS;

/**
 * My compaction algorithm configuration
 * @author ibra
 */
public final class IbraCompactionConfiguration extends AbstractCompactionConfiguration {

  private static final Log LOG = LogFactory.getLog(IbraCompactionConfiguration.class.getName());

  /**
   * @param hdfs HDFS object - represents HDFS - to read and write with several threads. 
   *        There is one HDFS object per Store
   * @param store - Store to initialize Compactor - store where this Compactor will work
   * @return corresponding Compactor for this CompactionConfiguration
   */
  @Override
  public IbraCompactor getCompactor(final HDFS hdfs, final Store store) {
    return new IbraCompactor(hdfs, this, store);
  }

  /**
   * minimum number of files to compact
   */
  private long minFilesToCompact = 10;

  /**
   * ratio defines how much different files may be in the same compaction used in
   * IbraCompactor.isok
   */
  private long filesSimilarityRatio = 3;

  /**
   * fill getFields map
   */
  @Override
  protected Map<String, GetMethod> fillGetFields() {// @formatter:off
    final Map<String, GetMethod> res = super.fillGetFields();
    res.put("Min files to compact",   new GetMethod() { @Override public String get() {return Long.toString(IbraCompactionConfiguration.this.getMinFilesToCompact   ());} });
    res.put("Files similarity ratio", new GetMethod() { @Override public String get() {return Long.toString(IbraCompactionConfiguration.this.getFilesSimilarityRatio());} });
    return res;
  }// @formatter:on

  /**
   * fill setFields map
   */
  @Override
  protected Map<String, SetMethod> fillSetFields() {// @formatter:off
    final Map<String, SetMethod> res = super.fillSetFields();
    res.put("Min files to compact",   new SetMethod() { @Override public void set(final String value) {IbraCompactionConfiguration.this.setMinFilesToCompact   (value);} });
    res.put("Files similarity ratio", new SetMethod() { @Override public void set(final String value) {IbraCompactionConfiguration.this.setFilesSimilarityRatio(value);} });
    return res;
  }// @formatter:on

  // ===================================================================
  // =============================<GETTERS>=============================
  // ===================================================================
  public long getMinFilesToCompact() {
    return this.minFilesToCompact;
  }

  public long getFilesSimilarityRatio() {
    return this.filesSimilarityRatio;
  }

  // ===================================================================
  // ============================</GETTERS>=============================
  // ===================================================================

  // ===================================================================
  // =============================<SETTERS>=============================
  // ===================================================================
  private void setMinFilesToCompact(final String minFilesToCompact) {
    this.minFilesToCompact = Long.parseLong(minFilesToCompact);
  }

  private void setFilesSimilarityRatio(final String filesSimilarityRatio) {
    this.filesSimilarityRatio = Long.parseLong(filesSimilarityRatio);
  }
  // ===================================================================
  // ============================</SETTERS>=============================
  // ===================================================================
}
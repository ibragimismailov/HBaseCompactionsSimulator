package Model.Compactors.CompactionConfigurations;

import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import Model.Compactors.LevelBasedCompactor;
import Model.HBaseElements.Store;
import Tools.HDFS;

/**
 * LevelDB compaction algorithm configuration
 */
public final class LevelBasedCompactionConfiguration extends AbstractCompactionConfiguration {

  private static final Log LOG = LogFactory.getLog(LevelBasedCompactionConfiguration.class
      .getName());

  /**
   * @param hdfs HDFS object - represents HDFS - to read and write with several threads. 
   *        There is one HDFS object per Store
   * @param store - Store to initialize Compactor - store where this Compactor will work
   * @return corresponding Compactor for this CompactionConfiguration
   */
  @Override
  public LevelBasedCompactor getCompactor(final HDFS hdfs, final Store store) {
    return new LevelBasedCompactor(hdfs, this, store);
  }

  /**
   * amount of files in level 0
   */
  private double level0Files = 5;

  /**
   * max byte size of sum of files in level i = level0ByteSize * pow(levelsSizeIncrease, i)
   */
  private double levelsSizeIncrease = 1.5;

  /**
   * this configuration defines how much different files can be on the same level
   */
  private long filesSimilarityRatio = 2;

  /**
   * maximum level id
   */
  private long levelsCount = 25;

  /**
   * fill setFields map
   */
  @Override
  protected Map<String, SetMethod> fillSetFields() {// @formatter:off
    final Map<String, SetMethod> res = super.fillSetFields();
    res.put("Level0 files",                   new SetMethod() { @Override public void set(final String value) {LevelBasedCompactionConfiguration.this.setLevel0Files         (value);} });
    res.put("Levels file max count increase", new SetMethod() { @Override public void set(final String value) {LevelBasedCompactionConfiguration.this.setLevelsSizeIncrease  (value);} });
    res.put("Files similarity ratio",         new SetMethod() { @Override public void set(final String value) {LevelBasedCompactionConfiguration.this.setFilesSimilarityRatio(value);} });
    res.put("Levels count",                   new SetMethod() { @Override public void set(final String value) {LevelBasedCompactionConfiguration.this.setLevelsCount         (value);} });
    return res;
  }// @formatter:on

  /**
   * fill getFields map
   */
  @Override
  protected Map<String, GetMethod> fillGetFields() {// @formatter:off
    final Map<String, GetMethod> res = super.fillGetFields();
    res.put("Level0 files",                   new GetMethod() { @Override public String get() {return Double.toString(LevelBasedCompactionConfiguration.this.getLevel0Files         ());} });
    res.put("Levels file max count increase", new GetMethod() { @Override public String get() {return Double.toString(LevelBasedCompactionConfiguration.this.getLevelsSizeIncrease  ());} });
    res.put("Files similarity ratio",         new GetMethod() { @Override public String get() {return Long  .toString(LevelBasedCompactionConfiguration.this.getFilesSimilarityRatio());} });
    res.put("Levels count",                   new GetMethod() { @Override public String get() {return Long  .toString(LevelBasedCompactionConfiguration.this.getLevelsCount         ());} });
    return res;
  };// @formatter:on

  // ===================================================================
  // =============================<GETTERS>=============================
  // ===================================================================
  public double getLevel0Files() {
    return this.level0Files;
  }

  public double getLevelsSizeIncrease() {
    return this.levelsSizeIncrease;
  }

  public long getFilesSimilarityRatio() {
    return this.filesSimilarityRatio;
  }

  public long getLevelsCount() {
    return this.levelsCount;
  }

  // ===================================================================
  // ============================</GETTERS>=============================
  // ===================================================================

  // ===================================================================
  // =============================<SETTERS>=============================
  // ===================================================================
  private void setLevel0Files(final String level0Files) {
    this.level0Files = Double.parseDouble(level0Files);
  }

  private void setLevelsSizeIncrease(final String levelsSizeIncrease) {
    this.levelsSizeIncrease = Double.parseDouble(levelsSizeIncrease);
  }

  private void setFilesSimilarityRatio(final String filesSimilarityRatio) {
    this.filesSimilarityRatio = Long.parseLong(filesSimilarityRatio);
  }

  private void setLevelsCount(final String levelsCount) {
    this.levelsCount = Long.parseLong(levelsCount);
  }
  // ===================================================================
  // ============================</SETTERS>=============================
  // ===================================================================
}
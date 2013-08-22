package Model.Compactors.CompactionConfigurations;

import java.util.Map;
import java.util.TreeMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import GUI.ConfigurationFrames.AbstractConfigurationFrame;
import GUI.ConfigurationFrames.CompactionConfigurationFrame;
import Model.AbstractConfiguration;
import Model.Compactors.AbstractCompactor;
import Model.HBaseElements.Store;
import Tools.HDFS;

/**
 * Each compaction algorithm has its own compaction configuration class
 * @author ibra
 */
public abstract class AbstractCompactionConfiguration extends AbstractConfiguration {

  private static final Log LOG = LogFactory.getLog(AbstractCompactionConfiguration.class.getName());

  /**
   * Title of this configuration - name for it 
   */
  private String title = this.getClass().getSimpleName();

  /**
   * @param hdfs HDFS object - represents HDFS - to read and write with several threads. 
   *        There is one HDFS object per Store
   * @param store - Store to initialize Compactor - store where this Compactor will work
   * @return corresponding Compactor for this CompactionConfiguration
   */
  public abstract AbstractCompactor getCompactor(final HDFS hdfs, final Store store);

  @Override
  protected AbstractConfigurationFrame getConfigurationFrame(final String frameTitle,
      final Map<String, GetMethod> getFields, final Map<String, SetMethod> setFields) {
    return new CompactionConfigurationFrame("Store '" + frameTitle + "' CompactionConfiguration",
        getFields, setFields);
  }

  /**
   * fill getFields map
   */
  @Override
  protected Map<String, GetMethod> fillGetFields() {// @formatter:off
    Map<String, GetMethod> res = new TreeMap<String, GetMethod> ();
    res.put("Title", new GetMethod() { @Override public String get() {return AbstractCompactionConfiguration.this.getTitle();} });
    return res;
  }// @formatter:on

  /**
   * fill setFields map
   */
  @Override
  protected Map<String, SetMethod> fillSetFields() {// @formatter:off
    Map<String, SetMethod> res = new TreeMap<String, SetMethod> ();
    res.put("Title", new SetMethod() { @Override public void set(final String value) {AbstractCompactionConfiguration.this.setTitle(value);} });
    return res;
  }// @formatter:on

  // ===================================================================
  // =============================<GETTERS>=============================
  // ===================================================================
  public String getTitle() {
    return this.title;
  }

  // ===================================================================
  // ============================</GETTERS>=============================
  // ===================================================================

  // ===================================================================
  // =============================<SETTERS>=============================
  // ===================================================================
  protected void setTitle(String title) {
    this.title = title;
  }
  // ===================================================================
  // ============================</SETTERS>=============================
  // ===================================================================
}
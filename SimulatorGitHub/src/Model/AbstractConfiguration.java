package Model;

import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import GUI.ConfigurationFrames.AbstractConfigurationFrame;

/**
 * abstract parent class for Configuration and different CompactionConfigurations
 * @author ibra
 *
 */
public abstract class AbstractConfiguration {

  private static final Log LOG = LogFactory.getLog(AbstractConfiguration.class.getName());

  /**
   * All methods to get compaction configuration fields. They are organized to Map it to be easy to
   * add/remove some configurations: to add a new configuration all you need to do is create
   * private field, getter, setter and add it to this map in overloaded fillGetFields method
   */
  private final Map<String, GetMethod> getFields = this.fillGetFields();

  /**
   * All methods to set compaction configuration fields. They are organized to Map it to be easy to
   * add/remove some configurations: to add a new configuration all you need to do is create
   * private field, getter, setter and add it to this map in overloaded fillGetFields method
   */
  private final Map<String, SetMethod> setFields = this.fillSetFields();

  /**
   * fill getFields map
   */
  protected abstract Map<String, GetMethod> fillGetFields();

  /**
   * fill setFields map
   */
  protected abstract Map<String, SetMethod> fillSetFields();

  /**
   * methods updates compaction configuration using CompactionConfigurationFrame this method is called
   * from ComfigurationFrame if user hits the button to change some compaction configuration
   * @param title - title for ConfigurationSetterFrame
   */
  public final void showFrame(final String title) {
    this.getConfigurationFrame(title, this.getFields, this.setFields).setVisible(true);
  }

  /**
   * abstract method to get ConfigurationFrame for changing this configuration
   * @param title - main title for ConfigurationFrame
   * @param getFields - get fields map of compaction configuration
   * @param setFields - get fields map of compaction configuration
   */
  protected abstract AbstractConfigurationFrame getConfigurationFrame(final String title,
      final Map<String, GetMethod> getFields, final Map<String, SetMethod> setFields);

  /**
   * interface to get compaction configuration fields
   */
  public interface GetMethod {
    public String get();
  }

  /**
   * interface to set compaction configuration fields
   */
  public interface SetMethod {
    public void set(String value) throws NumberFormatException;
  }
}

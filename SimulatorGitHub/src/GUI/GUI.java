package GUI;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import Model.Configuration;

/**
 * Main class
 * @author ibra
 */
public class GUI {

  private static final Log LOG = LogFactory.getLog(GUI.class.getName());

  public static void main(final String args[]) {
    Configuration.INSTANCE.showFrame("Configuration frame");
  }
}

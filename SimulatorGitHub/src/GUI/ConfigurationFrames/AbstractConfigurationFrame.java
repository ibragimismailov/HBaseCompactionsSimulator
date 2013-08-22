package GUI.ConfigurationFrames;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import javax.swing.GroupLayout;
import javax.swing.GroupLayout.ParallelGroup;
import javax.swing.GroupLayout.SequentialGroup;
import javax.swing.JComponent;
import javax.swing.JLabel;
import javax.swing.JTextField;
import javax.swing.LayoutStyle;

import Model.AbstractConfiguration.GetMethod;
import Model.AbstractConfiguration.SetMethod;

/**
 * AbstractConfigurationFrame is abstract parent class for ConfigurationFrame and CompactionConfigurationFrame
 * 
 * @author ibra
 */
public abstract class AbstractConfigurationFrame extends javax.swing.JFrame {

  /**
   * Labels and TextFields for configuration values
   */
  protected List<JLabel> labels;
  protected List<JTextField> fields;

  /**
   * Configuration fields getters and setters - used for getting configuration values and setting
   * them this way is better that just having configuration object here and changing it through
   * public get and set methods, because this way is more safe and more encapsulated: this way makes
   * it very easier and safer if you will need to add or remove some configuration to some
   * compaction configuration
   */
  protected Map<String, GetMethod> getFields;
  protected Map<String, SetMethod> setFields;

  /**
   * This methods just goes through configuration values and 
   *   checks if values user entered to textFields are correct
   * @return List of configuration values titles, which 
   *   textFields contain incorrect values entered by user 
   */
  protected List<String> getFieldsBadValues() {
    List<String> badValues = new ArrayList<String>();
    for (int i = 0; i < this.labels.size(); i++) {
      String confTitle = this.labels.get(i).getText();
      try {
        this.setFields.get(confTitle).set(this.fields.get(i).getText());
      } catch (NumberFormatException exc) {
        badValues.add(confTitle);
      }
    }
    return badValues;
  }

  /**
   * This method creates ParallelGroup with JComponents from comps
   * @param layout - GroupLayout, we create ParallelGroup for
   * @param comps - List<JComponents> for ParallelGroup
   * @param min - minimal Size for JComponents
   * @param pref - preferred Size for JComponents
   * @param max - maximal Size for JComponents
   * @return ParallelGroup with JComponents from comps
   */
  protected static ParallelGroup createParallelGroup(final GroupLayout layout,
      final List<? extends JComponent> comps, int min, int pref, int max) {
    ParallelGroup res = layout.createParallelGroup(GroupLayout.Alignment.LEADING);
    for (JComponent comp : comps) {
      res = res.addComponent(comp, min, pref, max);
    }
    return res;
  }

  /**
   * This method creates SequentialGroup with pairs of JComponents 
   * on each line (leftComps[i], rightComps[i])
   * @param layout - GroupLayout, we create ParallelGroup for
   * @param leftComps - List<JComponents> for ParallelGroup on left side
   * @param rightComps - List<JComponents> for ParallelGroup on right side
   * @param min - minimal Size for JComponents
   * @param pref - preferred Size for JComponents
   * @param max - maximal Size for JComponents
   * @return ParallelGroup with JComponents from leftComps and rightComps
   */
  protected static SequentialGroup createSequentialGroup(final GroupLayout layout,
      final List<? extends JComponent> leftComps, final List<? extends JComponent> rightComps,
      int min, int pref, int max) {
    SequentialGroup res = layout.createSequentialGroup();
    for (int i = 0; i < leftComps.size(); i++) {
      res = res.addGroup(
        layout.createParallelGroup(GroupLayout.Alignment.BASELINE).addComponent(leftComps.get(i))
            .addComponent(rightComps.get(i), min, pref, max)).addPreferredGap(
        LayoutStyle.ComponentPlacement.RELATED);
    }
    return res;
  }
}

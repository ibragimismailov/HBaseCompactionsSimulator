package GUI.ConfigurationFrames;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import javax.swing.GroupLayout;
import javax.swing.JLabel;
import javax.swing.JOptionPane;
import javax.swing.JTextField;
import javax.swing.LayoutStyle;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import Model.AbstractConfiguration.GetMethod;
import Model.AbstractConfiguration.SetMethod;

/**
 * When user clicks button that is responsible for some compaction configuration (right part of
 * ConfigurationFrame), this frame is showed to change this compaction algorithm configuration
 * @author ibra
 */
public class CompactionConfigurationFrame extends AbstractConfigurationFrame {

  private static final Log LOG = LogFactory.getLog(CompactionConfigurationFrame.class.getName());

  /**
   * apply button - applies changes user entered to compaction configuration
   */
  private javax.swing.JButton applyButton;

  /**
   * cancel button - cancels changes user entered to compaction configuration
   */
  private javax.swing.JButton cancelButton;

  /**
   * panel, that contains all labels and fields
   */
  private javax.swing.JPanel mainPanel;

  /**
   * panel that contains apply button and cancel button
   */
  private javax.swing.JPanel applyCancelPanel;

  /**
   * @param title - main title of frame
   * @param getFields - get fields map of compaction configuration
   * @param setFields - get fields map of compaction configuration
   */
  public CompactionConfigurationFrame(final String title, final Map<String, GetMethod> getFields,
      final Map<String, SetMethod> setFields) {
    this.initComponents(title, getFields, setFields);
  }

  /**
   * initializes and layouts all gui components
   * @param title - main title of frame
   * @param getFields - get fields map of compaction configuration
   * @param setFields - get fields map of compaction configuration
   */
  private void initComponents(final String title, final Map<String, GetMethod> getFields,
      final Map<String, SetMethod> setFields) {
    this.mainPanel = new javax.swing.JPanel();
    this.applyCancelPanel = new javax.swing.JPanel();
    this.applyButton = new javax.swing.JButton();
    this.cancelButton = new javax.swing.JButton();
    this.getFields = getFields;
    this.setFields = setFields;

    this.labels = new ArrayList<JLabel>();
    this.fields = new ArrayList<JTextField>();

    this.setDefaultCloseOperation(javax.swing.WindowConstants.DISPOSE_ON_CLOSE);
    this.setTitle(title);

    this.mainPanel.setBackground(new java.awt.Color(204, 204, 255));

    for (final String s : this.getFields.keySet()) {
      this.labels.add(new JLabel(s));
      this.fields.add(new JTextField(this.getFields.get(s).get()));
    }

    // @formatter:off
    final GroupLayout mainPanelLayout = new GroupLayout(this.mainPanel);
    this.mainPanel.setLayout(mainPanelLayout);
    mainPanelLayout.setHorizontalGroup(mainPanelLayout.createSequentialGroup()
      .addContainerGap()
      .addGroup(createParallelGroup(mainPanelLayout, this.labels, GroupLayout.DEFAULT_SIZE, GroupLayout.DEFAULT_SIZE, GroupLayout.PREFERRED_SIZE))
      .addGap(40, 40, 40)
      .addGroup(createParallelGroup(mainPanelLayout, this.fields, GroupLayout.DEFAULT_SIZE, 300, GroupLayout.PREFERRED_SIZE))
      .addContainerGap()
    );
    mainPanelLayout.setVerticalGroup(
      createSequentialGroup(mainPanelLayout, this.labels, this.fields, GroupLayout.DEFAULT_SIZE, GroupLayout.DEFAULT_SIZE, GroupLayout.PREFERRED_SIZE)
    );
    // @formatter:on

    this.applyCancelPanel.setBackground(new java.awt.Color(153, 153, 255));

    this.applyButton.setText("OK");
    this.applyButton.addActionListener(new java.awt.event.ActionListener() {
      @Override
      public void actionPerformed(final java.awt.event.ActionEvent evt) {
        CompactionConfigurationFrame.this.applyButtonActionPerformed(evt);
      }
    });

    this.cancelButton.setText("Cancel");
    this.cancelButton.addActionListener(new java.awt.event.ActionListener() {
      @Override
      public void actionPerformed(final java.awt.event.ActionEvent evt) {
        CompactionConfigurationFrame.this.cancelButtonActionPerformed(evt);
      }
    });

    // @formatter:off
    final GroupLayout applyCancelPanelLayout = new GroupLayout(this.applyCancelPanel);
    this.applyCancelPanel.setLayout(applyCancelPanelLayout);
    applyCancelPanelLayout.setHorizontalGroup(applyCancelPanelLayout.createSequentialGroup()
      .addGap(0, 188, Short.MAX_VALUE)
      .addComponent(this.cancelButton)
      .addPreferredGap(LayoutStyle.ComponentPlacement.RELATED)
      .addComponent(this.applyButton)
    );
    // @formatter:on

    applyCancelPanelLayout.setVerticalGroup(applyCancelPanelLayout
        .createParallelGroup(GroupLayout.Alignment.BASELINE).addComponent(this.applyButton)
        .addComponent(this.cancelButton));

    // @formatter:off
    final GroupLayout layout = new GroupLayout(this.getContentPane());
    this.getContentPane().setLayout(layout);
    layout.setHorizontalGroup(layout.createSequentialGroup()
      .addContainerGap()
      .addGroup(layout.createParallelGroup(GroupLayout.Alignment.LEADING)
        .addComponent(this.mainPanel, GroupLayout.DEFAULT_SIZE, GroupLayout.DEFAULT_SIZE, Short.MAX_VALUE)
        .addComponent(this.applyCancelPanel, GroupLayout.DEFAULT_SIZE, GroupLayout.DEFAULT_SIZE, Short.MAX_VALUE)
      )
      .addContainerGap()
    );
    layout.setVerticalGroup(layout.createSequentialGroup()
      .addContainerGap()
      .addComponent(this.mainPanel, GroupLayout.DEFAULT_SIZE, GroupLayout.DEFAULT_SIZE, Short.MAX_VALUE)
      .addPreferredGap(LayoutStyle.ComponentPlacement.RELATED)
      .addComponent(this.applyCancelPanel, GroupLayout.PREFERRED_SIZE, GroupLayout.DEFAULT_SIZE, GroupLayout.PREFERRED_SIZE)
      .addContainerGap()
    );
    // @formatter:on

    this.pack();
  }

  /**
   * Method sets values from textFields to compaction configuration
   */
  private void applyButtonActionPerformed(final java.awt.event.ActionEvent evt) {
    List<String> badValues = this.getFieldsBadValues();

    if (badValues.isEmpty()) {
      JOptionPane.showMessageDialog(this, "Applied!");
      this.setVisible(false);
      this.dispose();
    } else {
      JOptionPane.showMessageDialog(this, "Values, you entered to fields\n"
          + badValues.toString().replace(", ", ",\n") + "\n" + "are incorrect");

      // restore correct values of fields
      for (int i = 0; i < this.labels.size(); i++) {
        String key = this.labels.get(i).getText();
        this.fields.get(i).setText(this.getFields.get(key).get());
      }
    }
  }

  /**
   * method cancels user changes to compaction configuration by closing frame without applying
   * changes
   */
  private void cancelButtonActionPerformed(final java.awt.event.ActionEvent evt) {
    this.setVisible(false);
    this.dispose();
  }
}
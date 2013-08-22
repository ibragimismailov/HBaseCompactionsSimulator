package GUI.ConfigurationFrames;

import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import javax.swing.BorderFactory;
import javax.swing.GroupLayout;
import javax.swing.JButton;
import javax.swing.JComboBox;
import javax.swing.JLabel;
import javax.swing.JOptionPane;
import javax.swing.JPanel;
import javax.swing.JTextField;
import javax.swing.LayoutStyle;
import javax.swing.WindowConstants;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import Model.AbstractConfiguration.GetMethod;
import Model.AbstractConfiguration.SetMethod;
import Model.Configuration;
import Model.Simulator;
import Model.Compactors.CompactionAlgorithm;
import Model.Compactors.CompactionConfigurations.AbstractCompactionConfiguration;

/**
 * Configuration Frame is main frame of application. It provides GUI for changing simulator settings: 
 * left side of frame contains general HBase configurations from Configuration class
 * right side of frame contains configurations for all compaction configurations 
 * @author ibra
 */
public class ConfigurationFrame extends AbstractConfigurationFrame {

  private static final Log LOG = LogFactory.getLog(ConfigurationFrame.class.getName());

  /**
   * Compaction configuration of each compaction algorithm
   */
  private final List<AbstractCompactionConfiguration> compactorsConfigurations;

  /**
   * buttons whose action is to change compaction configurations of each compaction algorithm
   */
  private final List<JButton> storeButtons;

  /**
   * ComboBoxes for choosing type of compaction algorithm for each store
   */
  private final List<JComboBox> storeCombos;

  /**
   * apply button - for applying changes to Configuration, user did in general HBase configurations
   */
  private JButton applyButton;

  /**
   * go button - for starting HBase simulator
   */
  private JButton goButton;

  /**
   * stop button - for stopping HBase simulator
   */
  private JButton stopButton;

  /**
   * creates and initializes ConfigurationFrame object
   * @param title - main title of frame
   * @param getFields - get fields map of compaction configuration
   * @param setFields - get fields map of compaction configuration
   */
  public ConfigurationFrame(final String title, final Map<String, GetMethod> getFields,
      final Map<String, SetMethod> setFields) {
    this.compactorsConfigurations = new ArrayList<AbstractCompactionConfiguration>();
    this.storeButtons = new ArrayList<JButton>();
    this.storeCombos = new ArrayList<JComboBox>();

    // initialize all compaction algorithms, its buttons and comboboxes
    int[] graphsIds = new int[Configuration.INSTANCE.MAX_COMPACTION_ALGOS_COUNT];
    for (int i = 0; i < Configuration.INSTANCE.MAX_COMPACTION_ALGOS_COUNT; i++) {
      graphsIds[i] = i;
    }
    final CompactionAlgorithm[] compactionAlgorithms = CompactionAlgorithm.values();
    for (final int i : graphsIds) {
      final AbstractCompactionConfiguration compactionConfiguration = compactionAlgorithms[0]
          .getCompactionConfiguration();
      final JButton button = new JButton(Integer.toString(i));
      final JComboBox comboBox = new JComboBox(compactionAlgorithms);

      button.addActionListener(new ActionListener() {
        @Override
        public void actionPerformed(final ActionEvent e) {
          ConfigurationFrame.this.compactorsConfigurations.get(i).showFrame(button.getText());
        }
      });
      comboBox.addActionListener(new ActionListener() {
        @Override
        public void actionPerformed(final ActionEvent arg0) {
          ConfigurationFrame.this.compactorsConfigurations.set(i,
            ((CompactionAlgorithm) comboBox.getSelectedItem()).getCompactionConfiguration());
        }
      });

      this.compactorsConfigurations.add(compactionConfiguration);
      this.storeButtons.add(button);
      this.storeCombos.add(comboBox);
    }
    this.initComponents(title, getFields, setFields);
    this.updateStores();
  }

  /**
   * initialize and layout gui components
   */
  private void initComponents(final String title, final Map<String, GetMethod> getFields,
      final Map<String, SetMethod> setFields) {
    final JPanel jPanel1 = new JPanel();
    final JPanel jPanel2 = new JPanel();
    final JPanel jPanel3 = new JPanel();

    this.applyButton = new JButton();
    this.goButton = new JButton();
    this.stopButton = new JButton();
    this.getFields = getFields;
    this.setFields = setFields;

    this.labels = new ArrayList<JLabel>();
    this.fields = new ArrayList<JTextField>();

    this.setDefaultCloseOperation(WindowConstants.EXIT_ON_CLOSE);
    this.setTitle(title);
    this.setResizable(false);

    jPanel1.setBackground(new java.awt.Color(165, 165, 165));
    jPanel1.setBorder(BorderFactory.createTitledBorder("Genaral configurations"));

    for (String key : getFields.keySet()) {
      this.labels.add(new JLabel(key));
      this.fields.add(new JTextField(getFields.get(key).get()));
    }

    // @formatter:off
    final GroupLayout jPanel1Layout = new GroupLayout(jPanel1);
    jPanel1.setLayout(jPanel1Layout);
    jPanel1Layout.setHorizontalGroup(
      jPanel1Layout.createSequentialGroup()
      .addContainerGap()
      .addGroup(createParallelGroup(jPanel1Layout, this.labels, GroupLayout.DEFAULT_SIZE, GroupLayout.DEFAULT_SIZE, GroupLayout.PREFERRED_SIZE))
      .addGap(40, 40, 40)
      .addGroup(createParallelGroup(jPanel1Layout, this.fields, GroupLayout.DEFAULT_SIZE, 300, GroupLayout.PREFERRED_SIZE))
      .addContainerGap()
    );
    jPanel1Layout.setVerticalGroup(
      createSequentialGroup(jPanel1Layout, this.labels, this.fields, GroupLayout.DEFAULT_SIZE, GroupLayout.DEFAULT_SIZE, GroupLayout.PREFERRED_SIZE)
    );
    // @formatter:on
    jPanel2.setBackground(new java.awt.Color(165, 165, 165));
    jPanel2.setBorder(BorderFactory.createTitledBorder("Store compaction configurations"));

    // @formatter:off
    final GroupLayout jPanel2Layout = new GroupLayout(jPanel2);
    jPanel2.setLayout(jPanel2Layout);
    jPanel2Layout.setHorizontalGroup(
      jPanel2Layout.createSequentialGroup()
      .addContainerGap()
      .addGroup(createParallelGroup(jPanel2Layout, this.storeButtons, GroupLayout.DEFAULT_SIZE, GroupLayout.DEFAULT_SIZE, GroupLayout.PREFERRED_SIZE))
      .addGap(40, 40, 40)
      .addGroup(createParallelGroup(jPanel2Layout, this.storeCombos, GroupLayout.DEFAULT_SIZE, GroupLayout.DEFAULT_SIZE, GroupLayout.PREFERRED_SIZE))
      .addContainerGap()
    );
    jPanel2Layout.setVerticalGroup(
      createSequentialGroup(jPanel2Layout, this.storeButtons, this.storeCombos, GroupLayout.DEFAULT_SIZE, GroupLayout.DEFAULT_SIZE, GroupLayout.PREFERRED_SIZE)
    );
    // @formatter:on

    jPanel3.setBorder(BorderFactory.createEtchedBorder());

    this.applyButton.setText("Apply");
    this.applyButton.addActionListener(new java.awt.event.ActionListener() {
      @Override
      public void actionPerformed(final java.awt.event.ActionEvent evt) {
        ConfigurationFrame.this.applyActionPerformed(evt);
      }
    });

    this.goButton.setText("Go!");
    this.goButton.addActionListener(new java.awt.event.ActionListener() {
      @Override
      public void actionPerformed(final java.awt.event.ActionEvent evt) {
        ConfigurationFrame.this.goActionPerformed(evt);
      }
    });

    this.stopButton.setText("Stop");
    this.stopButton.setVisible(false);
    this.stopButton.addActionListener(new java.awt.event.ActionListener() {
      @Override
      public void actionPerformed(final java.awt.event.ActionEvent evt) {
        ConfigurationFrame.this.stopActionPerformed(evt);
      }
    });

    // @formatter:off
    final GroupLayout jPanel3Layout = new GroupLayout(jPanel3);
    jPanel3.setLayout(jPanel3Layout);
    jPanel3Layout.setHorizontalGroup(
      jPanel3Layout.createSequentialGroup()
      .addContainerGap(GroupLayout.DEFAULT_SIZE, Short.MAX_VALUE)
      .addComponent(this.goButton,    GroupLayout.PREFERRED_SIZE, 150, GroupLayout.PREFERRED_SIZE)
      .addPreferredGap(LayoutStyle.ComponentPlacement.RELATED)
      .addComponent(this.stopButton,  GroupLayout.PREFERRED_SIZE, 150, GroupLayout.PREFERRED_SIZE)
      .addPreferredGap(LayoutStyle.ComponentPlacement.RELATED)
      .addComponent(this.applyButton, GroupLayout.PREFERRED_SIZE, 150, GroupLayout.PREFERRED_SIZE)
      .addContainerGap()
    );
    jPanel3Layout.setVerticalGroup(
      jPanel3Layout.createParallelGroup(GroupLayout.Alignment.LEADING)
      .addComponent(this.applyButton, GroupLayout.DEFAULT_SIZE, 100, Short.MAX_VALUE)
      .addComponent(this.goButton,    GroupLayout.DEFAULT_SIZE, 100, Short.MAX_VALUE)
      .addComponent(this.stopButton,  GroupLayout.DEFAULT_SIZE, 100, Short.MAX_VALUE)
    );
    // @formatter:on

    // @formatter:off
    final GroupLayout layout = new GroupLayout(this.getContentPane());
    this.getContentPane().setLayout(layout);
    layout.setHorizontalGroup(layout.createSequentialGroup()
      .addContainerGap()
      .addGroup(layout.createParallelGroup(GroupLayout.Alignment.LEADING)
        .addComponent(jPanel3, GroupLayout.DEFAULT_SIZE, GroupLayout.DEFAULT_SIZE, Short.MAX_VALUE)
        .addGroup(layout.createSequentialGroup()
          .addComponent(jPanel1, GroupLayout.PREFERRED_SIZE, GroupLayout.DEFAULT_SIZE, GroupLayout.PREFERRED_SIZE)
          .addPreferredGap(LayoutStyle.ComponentPlacement.RELATED)
          .addComponent(jPanel2, GroupLayout.DEFAULT_SIZE, GroupLayout.DEFAULT_SIZE, Short.MAX_VALUE)
        )
      )
      .addContainerGap()
    );
    layout.setVerticalGroup(layout.createSequentialGroup()
      .addContainerGap()
      .addGroup(layout.createParallelGroup(GroupLayout.Alignment.TRAILING)
        .addComponent(jPanel1, GroupLayout.DEFAULT_SIZE, GroupLayout.DEFAULT_SIZE, Short.MAX_VALUE)
        .addComponent(jPanel2, GroupLayout.DEFAULT_SIZE, GroupLayout.DEFAULT_SIZE, Short.MAX_VALUE)
      )
      .addPreferredGap(LayoutStyle.ComponentPlacement.RELATED)
      .addComponent(jPanel3, GroupLayout.PREFERRED_SIZE, GroupLayout.DEFAULT_SIZE, GroupLayout.PREFERRED_SIZE)
      .addContainerGap(GroupLayout.DEFAULT_SIZE, Short.MAX_VALUE)
    );
    // @formatter:on

    this.pack();
  }

  /**
   * Apply configuration values user entered to general HBase configuration
   */
  private void applyActionPerformed(final java.awt.event.ActionEvent evt) {
    final List<String> badValues = this.getFieldsBadValues();

    if (badValues.isEmpty()) {
      this.updateStores();
      JOptionPane.showMessageDialog(this, "Applied!");
    } else {
      JOptionPane.showMessageDialog(this, "Values, you entered to fields\n"
          + badValues.toString().replace(", ", ",\n") + "\n" + "are incorrect");

      // restore correct values of fields
      for (int i = 0; i < this.labels.size(); i++) {
        String key = this.labels.get(i).getText();
        this.fields.get(i).setText(this.getFields.get(key).get());
      }
      this.updateStores();
    }
  }

  /**
   * Start simulator
   */
  public void goActionPerformed(final java.awt.event.ActionEvent evt) {
    this.goButton.setVisible(false);
    this.stopButton.setVisible(true);

    new Thread(new Runnable() {
      @Override
      public void run() {
        Simulator.INSTANCE.start(ConfigurationFrame.this.compactorsConfigurations);
      }
    }).start();
  }

  /**
   * Stop simulator
   */
  private void stopActionPerformed(final java.awt.event.ActionEvent evt) {
    this.stopButton.setVisible(false);
    Simulator.INSTANCE.stop();
  }

  /**
   * Update amount of stores, show/hide new/redundant store's compaction configurations
   */
  private void updateStores() {
    for (int i = 0; i < this.compactorsConfigurations.size(); i++) {
      this.storeButtons.get(i).setVisible(i < Configuration.INSTANCE.getCompactionAlgosCount());
      this.storeCombos.get(i).setVisible(i < Configuration.INSTANCE.getCompactionAlgosCount());
    }
  }
}

package Model.Compactors;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import Model.Compactors.CompactionConfigurations.AbstractCompactionConfiguration;
import Model.Compactors.CompactionConfigurations.IbraCompactionConfiguration;
import Model.Compactors.CompactionConfigurations.LevelBasedCompactionConfiguration;
import Model.Compactors.CompactionConfigurations.RandomCompactionConfiguration;
import Model.Compactors.CompactionConfigurations.HBaseCompactionConfigurations.DefaultHBaseCompactionConfiguration;
import Model.Compactors.CompactionConfigurations.HBaseCompactionConfigurations.HBaseCompactionSpecific2Configuration;
import Model.Compactors.CompactionConfigurations.HBaseCompactionConfigurations.HBaseCompactionRandomConfiguration;
import Model.Compactors.CompactionConfigurations.HBaseCompactionConfigurations.HBaseCompactionSpecific1Configuration;

/**
 * all compaction algorithms
 * @author ibra
 */
public enum CompactionAlgorithm {
  HBaseCompactorWithDefaultConfiguration {
    @Override
    public AbstractCompactionConfiguration getCompactionConfiguration() {
      return new DefaultHBaseCompactionConfiguration();
    }
  },
  HBaseCompactorWithSpecific1Configuration {
    @Override
    public AbstractCompactionConfiguration getCompactionConfiguration() {
      return new HBaseCompactionSpecific1Configuration();
    }
  },
  HBaseCompactorWithSpecific2Configuration {
    @Override
    public AbstractCompactionConfiguration getCompactionConfiguration() {
      return new HBaseCompactionSpecific2Configuration();
    }
  },
  HBaseCompactorWithRandomConfiguration {
    @Override
    public AbstractCompactionConfiguration getCompactionConfiguration() {
      return new HBaseCompactionRandomConfiguration();
    }
  },
  LevelBasedCompactor {
    @Override
    public AbstractCompactionConfiguration getCompactionConfiguration() {
      return new LevelBasedCompactionConfiguration();
    }
  },
  IbraCompactor {
    @Override
    public AbstractCompactionConfiguration getCompactionConfiguration() {
      return new IbraCompactionConfiguration();
    }
  },
  RandomCompactor {
    @Override
    public AbstractCompactionConfiguration getCompactionConfiguration() {
      return new RandomCompactionConfiguration();
    }
  };

  private static final Log LOG = LogFactory.getLog(CompactionAlgorithm.class.getName());

  public abstract AbstractCompactionConfiguration getCompactionConfiguration();
}

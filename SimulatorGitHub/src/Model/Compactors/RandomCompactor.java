package Model.Compactors;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import Model.Compactors.CompactionConfigurations.RandomCompactionConfiguration;
import Model.HBaseElements.Store;
import Model.HBaseElements.StoreFileCollection;
import Tools.HDFS;
import Tools.RandomGenerator;

/**
 * very simple random compaction algorithm
 * @author ibra
 */
public class RandomCompactor extends AbstractCompactor {

  private static final Log LOG = LogFactory.getLog(RandomCompactor.class.getName());

  /**
   * compaction congifuration
   */
  private final RandomCompactionConfiguration compactionConfiguration;

  /**
   * creates and initializes object
   * @param hdfs HDFS object - represents HDFS - to read and write with several threads. 
   *        There is one HDFS object per Store
   * @param compactionConfiguration - this compactor configuration
   * @param store is Store object where this compactor works
   */
  public RandomCompactor(final HDFS hdfs,
      final RandomCompactionConfiguration compactionConfiguration, final Store store) {
    super(hdfs, store);
    this.compactionConfiguration = compactionConfiguration;
  }

  /**
   * methods selects what storeFiles should be compacted during minor compactions
   * @param storeFiles - all storeFiles of store
   * @return collection of StoreFiles to compact
   */
  @Override
  protected StoreFileCollection selectFilesToCompact(final StoreFileCollection storeFiles) {
    if (storeFiles.size() <= this.compactionConfiguration.getMaxFiles()) {
      return new StoreFileCollection();
    }

    final StoreFileCollection toCompact = new StoreFileCollection();
    while (toCompact.size() < this.compactionConfiguration.getFilesToCompact()) {
      int id = RandomGenerator.getRandomInt(storeFiles.size() - toCompact.size());
      for (int i = 0; i < storeFiles.size(); i++) {
        if (!toCompact.contains(storeFiles.get(i))) {
          id--;
        }
        if (id == -1) {
          toCompact.add(storeFiles.get(i));
          break;
        }
      }
    }
    return toCompact;
  }
}
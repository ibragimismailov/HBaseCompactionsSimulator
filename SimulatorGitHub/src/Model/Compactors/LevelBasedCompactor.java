package Model.Compactors;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import Model.Configuration;
import Model.Compactors.CompactionConfigurations.LevelBasedCompactionConfiguration;
import Model.HBaseElements.Store;
import Model.HBaseElements.StoreFileCollection;
import Tools.HDFS;

/**
 * Simplified LevelDB compaction algorithm:
 * During memStore flushes StoreFiles are flushed into Level0
 * then, when amount of files in i-th Level exceeds limit, we take all files in 
 * i-th level, compact them and put compacted file into (i+1)-th Level 
 * @author ibra
 */
public class LevelBasedCompactor extends AbstractCompactor {

  private static final Log LOG = LogFactory.getLog(LevelBasedCompactor.class.getName());

  /**
   * each compaction algorithm has its own configuration
   */
  private final LevelBasedCompactionConfiguration compactionConfiguration;

  /**
   * creates and initializes object
   * @param hdfs HDFS object - represents HDFS - to read and write with several threads. 
   *        There is one HDFS object per Store
   * @param compactionConfiguration - this compactor configuration
   * @param store is Store object where this compactor works
   */
  public LevelBasedCompactor(final HDFS hdfs,
      final LevelBasedCompactionConfiguration compactionConfiguration, final Store store) {
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
    long defFileSize = Configuration.INSTANCE.getMemstoreBytesSize()
        / Configuration.INSTANCE.COMPRESSION_RATIO;

    for (int level = 0; level < this.compactionConfiguration.getLevelsCount(); level++) {
      final long levelByteSize = this.getLevelSize(level, defFileSize);
      final long fileSize = level == 0 ? defFileSize : this.getLevelSize(level - 1, defFileSize);

      int start = -1, end = -1;

      for (int i = 0; i < storeFiles.size(); i++) {
        if (this.isAboutTheSame(fileSize, storeFiles.get(i).getBytesSize())) {
          start = i;
          break;
        }
      }
      for (int i = storeFiles.size() - 1; i >= 0; i--) {
        if (this.isAboutTheSame(fileSize, storeFiles.get(i).getBytesSize())) {
          end = i;
          break;
        }
      }

      if (start != -1 && end != -1) {
        long size = 0;
        for (int i = start; i <= end; i++) {
          size += storeFiles.get(i).getBytesSize();
        }
        if (size > levelByteSize) {
          return storeFiles.subList(start, end + 1);
        }
      }
    }
    return new StoreFileCollection();
  }

  /**
   * @param level - id of level
   * @return max byte size of sum of fileSizes on this level
   */
  private long getLevelSize(final int level, final long defFileSize) {
    return (long) (this.compactionConfiguration.getLevel0Files() * defFileSize * Math.pow(
      this.compactionConfiguration.getLevelsSizeIncrease(), level));
  }

  /**
   * @param fileSize1 - size of file1
   * @param fileSize2 - size of file2
   * @return if two files have about the same size
   */
  private boolean isAboutTheSame(final long fileSize1, final long fileSize2) {
    return Math.abs(fileSize1 - fileSize2) < Math.max(fileSize1, fileSize2)
        / this.compactionConfiguration.getFilesSimilarityRatio();
  }

}

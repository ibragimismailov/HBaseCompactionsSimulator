package Model.Compactors;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import Model.Simulator;
import Model.Compactors.CompactionConfigurations.HBaseCompactionConfigurations.HBaseCompactionConfiguration;
import Model.HBaseElements.Store;
import Model.HBaseElements.StoreFileCollection;
import Tools.HDFS;
import Tools.Helper;
import Tools.RandomGenerator;

/**
 * native compaction algorithm used in HBase
 * @author ibra
 */
public class HBaseCompactor extends AbstractCompactor {

  private static final Log LOG = LogFactory.getLog(HBaseCompactor.class.getName());

  /**
   * each compactor has own compaction configuration
   */
  private final HBaseCompactionConfiguration compactionConfiguration;

  /**
   * creates and initializes object
   * @param hdfs HDFS object - represents HDFS - to read and write with several threads. 
   *        There is one HDFS object per Store
   * @param compactionConfiguration - this compactor configuration
   * @param store is Store object where this compactor works
   */
  public HBaseCompactor(final HDFS hdfs,
      final HBaseCompactionConfiguration compactionConfiguration, final Store store) {
    super(hdfs, store);
    this.compactionConfiguration = compactionConfiguration;

    /**
     * thread that invokes major compactions
     */
    new Thread(new Runnable() {
      @Override
      public void run() {
        while (!Simulator.INSTANCE.isStopped()) {
          Helper.sleepTo(System.currentTimeMillis()
              + RandomGenerator.getMajorCompactionGap(
                HBaseCompactor.this.compactionConfiguration.getMajorCompactionsGap(),
                HBaseCompactor.this.compactionConfiguration.getMajorCompactionsJitter()));
          store.forceMajorCompaction();
        }
      }
    }).start();
  }

  /**
   * methods selects what storeFiles should be compacted during minor compactions
   * @param storeFiles - all storeFiles of store
   * @return collection of StoreFiles to compact
   */
  @Override
  protected StoreFileCollection selectFilesToCompact(final StoreFileCollection storeFiles) {
    StoreFileCollection toCompact = new StoreFileCollection(storeFiles);
    toCompact = this.skipLargeFiles(toCompact);
    toCompact = this.applyCompactionPolicy(toCompact);
    toCompact = this.checkMinFilesCriteria(toCompact);
    toCompact = this.removeExcessFiles(toCompact);

    return toCompact;
  }

  /**
   * method to determine is compaction that compacts files with this total size is large or small,
   * so it should be processed in large compactions thread or small compactions thread
   * @param totalSize - total size of files to be compacted during compaction
   * @return is compaction that compacts totalSize bytes considered to be large
   */
  @Override
  protected CompactionType getCompactionType(final long totalSize) {
    if (totalSize > this.compactionConfiguration.getThrottle()) {
      return CompactionType.LARGE;
    } else {
      return CompactionType.SMALL;
    }
  }

  /**
   * method removes too large storeFiles
   * @param storeFiles - collection of storeFiles
   * @return collection of storeFiles, without too large storeFiles
   */
  private StoreFileCollection skipLargeFiles(final StoreFileCollection storeFiles) {
    int pos = 0;
    while (pos < storeFiles.size()
        && storeFiles.get(pos).getBytesSize() > this.compactionConfiguration
            .getCompactionMaxBytes()) {
      pos++;
    }

    if (pos > 0) {
      return storeFiles.subList(pos, storeFiles.size());
    }

    return storeFiles;
  }

  /**
   * methods chooses storeFiles for compaction due to configuration
   * @param storeFiles - collection of storeFiles
   * @return collection of storeFiles to compact
   */
  private StoreFileCollection applyCompactionPolicy(StoreFileCollection storeFiles) {
    if (storeFiles.isEmpty()) {
      return storeFiles;
    }

    final double r = this.compactionConfiguration.getCompactionRatio();

    // get store file sizes for incremental compacting selection.
    final int countOfFiles = storeFiles.size();
    final long[] fileSizes = new long[countOfFiles];
    final long[] sumSize = new long[countOfFiles];
    for (int i = countOfFiles - 1; i >= 0; --i) {
      fileSizes[i] = storeFiles.get(i).getBytesSize();
      // calculate the sum of fileSizes[i,i+maxFilesToCompact-1) for algo
      final int tooFar = (int) (i + this.compactionConfiguration.getCompactionMaxFiles() - 1);
      sumSize[i] = fileSizes[i] + ((i + 1 < countOfFiles) ? sumSize[i + 1] : 0)
          - ((tooFar < countOfFiles) ? fileSizes[tooFar] : 0);
    }

    int start = 0;
    while (countOfFiles - start >= this.compactionConfiguration.getCompactionMinFiles()
        && fileSizes[start] > Math.max(this.compactionConfiguration.getCompactionMinBytes(),
          (long) (sumSize[start + 1] * r))) {
      ++start;
    }

    storeFiles = storeFiles.subList(start, countOfFiles);

    return storeFiles;
  }

  /**
   * method checks if storeFiles contains enough elements
   * @param storeFiles - collection of storeFiles
   * @return empty collection if storeFiles contains to few elements,
   *   else return collection from argument list
   */
  private StoreFileCollection checkMinFilesCriteria(final StoreFileCollection storeFiles) {
    // if storeFiles contains not enough files, then we return
    // empty StoreFilesCollection - that means that we won't do compaction
    if (storeFiles.size() < this.compactionConfiguration.getCompactionMinFiles()) {
      storeFiles.clear();
    }
    return storeFiles;
  }

  /**
   * if storeFiles collection contains too many elements, method removes excess storeFiles
   * @param storeFiles - collection of StoreFiles
   * @return collection with valid size
   */
  private StoreFileCollection removeExcessFiles(StoreFileCollection storeFiles) {
    final long excess = storeFiles.size() - this.compactionConfiguration.getCompactionMaxFiles();
    if (excess > 0) {
      storeFiles = storeFiles
          .subList(0, (int) this.compactionConfiguration.getCompactionMaxFiles());
    }
    return storeFiles;
  }
}

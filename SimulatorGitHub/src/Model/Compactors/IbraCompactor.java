package Model.Compactors;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import Model.Compactors.CompactionConfigurations.IbraCompactionConfiguration;
import Model.HBaseElements.Store;
import Model.HBaseElements.StoreFileCollection;
import Tools.HDFS;

/**
 * My compaction algorithm
 * It tries to compact as many almost-equal files as possible.
 * Algo tries to select the biggest set of files that fits this condition:
 * minFile * this.compactionConfiguration.getFilesSimilarityRatio() > maxFile;
 * @author ibra
 */
public class IbraCompactor extends AbstractCompactor {

  private static final Log LOG = LogFactory.getLog(IbraCompactor.class.getName());

  /**
   * each compaction algorithm has its own configuration
   */
  private final IbraCompactionConfiguration compactionConfiguration;

  /**
   * creates and initializes object
   * @param hdfs HDFS object - represents HDFS - to read and write with several threads. 
   *        There is one HDFS object per Store
   * @param compactionConfiguration - this compactor configuration
   * @param store is Store object where this compactor works
   */
  public IbraCompactor(final HDFS hdfs, final IbraCompactionConfiguration compactionConfiguration,
      final Store store) {
    super(hdfs, store);
    this.compactionConfiguration = compactionConfiguration;
  }

  /**
   * methods selects what storeFiles should be compacted during minor compaction
   * @param storeFiles - all storeFiles of store
   * @return collection of StoreFiles to compact
   */
  @Override
  protected StoreFileCollection selectFilesToCompact(final StoreFileCollection storeFiles) {
    /**
     * we select the longest sequence of files that differ in size not too much
     * if storeFiles[start] doestn't differ from storeFiles[i] in size, we 
     * sequence [start, i] can be taken (StoreFilesCollection elements are sorted 
     * by decreasing elements' bytesSize)
     */
    int bestStart = 0;
    int bestCount = 0;
    for (int start = 0; start < storeFiles.size(); start++) {
      int count = 1;
      for (int i = start + 1; i < storeFiles.size(); i++) {
        if (this.isok(storeFiles, start, i)) {
          count++;
        } else {
          break;
        }
      }
      if (count > bestCount) {
        bestCount = count;
        bestStart = start;
      }
    }

    final StoreFileCollection toCompact = new StoreFileCollection();
    for (int i = bestStart; i < bestStart + bestCount; i++) {
      toCompact.add(storeFiles.get(i));
    }

    if (toCompact.size() < this.compactionConfiguration.getMinFilesToCompact()) {
      return new StoreFileCollection();
    }

    return toCompact;
  }

  /**
   * method checks that files [start, i] have similar size
   * @param storeFiles - all storeFiles of Store
   * @param start - start index of range
   * @param i - end index of range
   * @return if files [start, i] have similar size
   */
  private boolean isok(final StoreFileCollection storeFiles, final int start, final int i) {
    final long a = storeFiles.get(start).getBytesSize();
    final long b = storeFiles.get(i).getBytesSize();
    return b * this.compactionConfiguration.getFilesSimilarityRatio() > a;
  }

}

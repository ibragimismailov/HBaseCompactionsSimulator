package Model.Compactors;

import java.util.Queue;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import Model.Configuration;
import Model.Simulator;
import Model.HBaseElements.Store;
import Model.HBaseElements.StoreFile;
import Model.HBaseElements.StoreFileCollection;
import Tools.HDFS;
import Tools.HDFSStream;
import Tools.Helper;

/**
 * AbstractCompactor is class for compactions. It does minor and major compactions.
 * There are 2 threads - for large compactions and small compactions 
 * You should inherit from this class if you want create some new compaction algorithm. 
 * AbstractCompactor contains compaction configuration object.
 * @author ibra
 */
public abstract class AbstractCompactor {

  private static final Log LOG = LogFactory.getLog(AbstractCompactor.class.getName());

  /**
   * store is Store object where this compactor works
   */
  private final Store store;

  /**
   * Background large compactor - does large compactions in background
   */
  private final BackgroundCompactor largeCompactor;

  /**
   * Background small compactor - does small compactions in background
   */
  private final BackgroundCompactor smallCompactor;

  /**
   * creates and initializes object
   * @param hdfs HDFS object - represents HDFS - to read and write with several threads. 
   *        There is one HDFS object per Store
   * @param store is Store object where this compactor works
   */
  public AbstractCompactor(final HDFS hdfs, final Store store) {
    this.largeCompactor = new BackgroundCompactor(hdfs);
    this.smallCompactor = new BackgroundCompactor(hdfs);
    this.store = store;
  }

  /**
   * methods selects what storeFiles should be compacted during minor compaction
   * @param storeFiles - all storeFiles of store
   * @return collection of StoreFiles to compact
   */
  protected abstract StoreFileCollection selectFilesToCompact(StoreFileCollection storeFiles);

  /**
   * method to determine is compaction that compacts files with this total size is large or small,
   * so it should be processed in large compactions thread or small compactions thread
   * @param totalSize - total size of files to be compacted during compaction
   * @return is compaction that compacts totalSize bytes considered to be large
   */
  protected CompactionType getCompactionType(final long totalSize) {
    return CompactionType.LARGE;
  }

  /**
   * method does minor compaction
   * @param storeFiles - collection of storeFiles of store
   */
  public final void doCompaction(final StoreFileCollection storeFiles) {
    final StoreFileCollection toCompact = this.selectFilesToCompact(storeFiles);
    this.compact(storeFiles, toCompact, false);
  }

  /**
   * method does major compaction
   * @param storeFiles - collection of storeFiles of store
   * @param store - store where compaction is being done
   */
  public final void forceMajorCompaction(final StoreFileCollection storeFiles) {
    this.compact(storeFiles, new StoreFileCollection(storeFiles), true);
  }

  /**
   * this method decides if compaction is large or small and adds it to corresponding BackgroundCompactor
   * @param storeFiles - collection of storeFiles of store
   * @param toCompact - collection of storeFiles that are to be compacted
   * @param isMajor - is compaction - a major compaction
   */
  private void compact(final StoreFileCollection storeFiles, final StoreFileCollection toCompact,
      boolean isMajor) {
    if (toCompact.isEmpty()) {
      return;
    }

    long compactionSize = 0;
    for (final StoreFile storeFile : toCompact) {
      compactionSize += storeFile.getBytesSize();
    }

    for (StoreFile storeFile : toCompact) {
      storeFiles.remove(storeFile);
    }

    switch (this.getCompactionType(compactionSize)) {
    case LARGE:
      this.largeCompactor.add(toCompact, compactionSize, isMajor);
      break;
    case SMALL:
      this.smallCompactor.add(toCompact, compactionSize, isMajor);
      break;
    }
  }

  /**
   * BackgroundCompactor class - does compactions in background
   * @author ibra
   */
  private class BackgroundCompactor {

    private final Log LOG = LogFactory.getLog(BackgroundCompactor.class.getName());

    /**
     * compactions queries queue
     */
    private final Queue<Query> queue = new LinkedBlockingQueue<Query>();

    /**
     * runs thread to do compactions
     */
    private BackgroundCompactor(final HDFS hdfs) {
      new Thread(new Runnable() {
        @Override
        public void run() {
          // HDFSStream - to read/write from HDFS in this thread
          final HDFSStream stream = new HDFSStream(hdfs);

          while (!Simulator.INSTANCE.isStopped()) {
            Helper.sleep();
            while (!BackgroundCompactor.this.queue.isEmpty()) {
              final StoreFileCollection toCompact = BackgroundCompactor.this.queue.peek().toCompact;
              long compactionSize = BackgroundCompactor.this.queue.peek().compactionSize;
              final boolean isMajor = BackgroundCompactor.this.queue.peek().isMajor;
              BackgroundCompactor.this.queue.poll();

              final long checkPoint = System.currentTimeMillis();

              // if this compaction is major, we take all compaction queries from
              // queue and do a major compaction - compaction of all files
              if (isMajor) {
                while (!BackgroundCompactor.this.queue.isEmpty()) {
                  compactionSize += BackgroundCompactor.this.queue.peek().compactionSize;
                  toCompact.addAll(BackgroundCompactor.this.queue.peek().toCompact);
                  BackgroundCompactor.this.queue.poll();
                }

                BackgroundCompactor.this.LOG.info("start major compaction");
                BackgroundCompactor.this.LOG.info("compactionSize =  " + compactionSize);
                BackgroundCompactor.this.logCompactionInfo(toCompact, toCompact);
              }

              StoreFile compacted = BackgroundCompactor.this.compact(toCompact, stream);
              // compaction finished, so we call compactionFinished method of Simulator
              // and send amount of data, that was read and written to HDFS during this compaction
              // compactionSize - was read
              // compacted.getBytesSize() - was written
              final long totalHDFSIO = compactionSize + compacted.getBytesSize();
              AbstractCompactor.this.store.compactionFinished(compacted, totalHDFSIO);

              if (isMajor) {
                BackgroundCompactor.this.LOG.info("it took "
                    + (double) (System.currentTimeMillis() - checkPoint)
                    * Configuration.INSTANCE.getxFaster() / Configuration.MS_PER_DAY
                    + " days. totalHDFSIO = " + totalHDFSIO);
                BackgroundCompactor.this.LOG.info("end major compaction");
              }
            }
          }
        }
      }).start();
    }

    /**
     * this method actually does compaction
     * @param toCompact - collection of storeFiles to compact
     * @param stream - HDFSStream to read/write from/to HDFS
     * @return compacted storeFile
     */
    private StoreFile compact(final StoreFileCollection toCompact, final HDFSStream stream) {
      final StoreFile compacted = new StoreFile();

      for (StoreFile storeFile : toCompact) {
        compacted.mergeWith(storeFile, stream);
      }

      return compacted;
    }

    /**
     * method adds compaction query to queue
     * @param toCompact - collect of StoreFiles to compact
     * @param compactionSize - size of this compaction (in bytes)
     * @param isMajor - is this compaction is major compaction
     */
    private void add(final StoreFileCollection toCompact, final long compactionSize,
        final boolean isMajor) {
      if (toCompact.isEmpty()) {
        return;
      }

      this.queue.add(new Query(toCompact, compactionSize, isMajor));
    }

    /**
     * prints some information about compaction
     * @param storeFiles - collection of storeFiles of store
     * @param toCompact - collection of storeFiles that are to be compacted
     */
    private void logCompactionInfo(final StoreFileCollection storeFiles,
        final StoreFileCollection toCompact) {
      String message = "";
      for (final StoreFile storeFile : storeFiles) {
        long size = (long) (storeFile.getBytesSize() * Configuration.INSTANCE.COMPRESSION_RATIO
            / Configuration.INSTANCE.getMemstoreBytesSize() + 0.01);
        if (toCompact.contains(storeFile)) {
          message += "[" + size + "]";
        } else {
          message += size;
        }
        message += " ";
      }
      this.LOG.info("");
      this.LOG.info(message);
    }

    /**
     * Compaction query class
     * @author ibra
     */
    private class Query {
      /**
       * collection of storeFiles to compact
       */
      private final StoreFileCollection toCompact;

      /**
       * is this compaction request - major compaction request
       */
      private final boolean isMajor;

      /**
       * overall size of file that are to be compacted
       */
      private final long compactionSize;

      /**
       * creates and initializes Query object
       * @param toCompact - storeFiles to compact
       * @param compactionSize - total size of StoreFiles in toCompact
       * @param isMajor - if this compaction request - major compaction request
       */
      private Query(final StoreFileCollection toCompact, final long compactionSize,
          final boolean isMajor) {
        this.toCompact = toCompact;
        this.compactionSize = compactionSize;
        this.isMajor = isMajor;
      }
    }
  }

  public enum CompactionType {
    SMALL, LARGE
  }
}
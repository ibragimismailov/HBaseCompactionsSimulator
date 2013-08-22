package Model.HBaseElements;

import java.util.Deque;
import java.util.Queue;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import Model.Simulator;
import Model.Compactors.AbstractCompactor;
import Tools.HDFS;
import Tools.HDFSStream;
import Tools.Helper;

/**
 * Store - consists of Collection of StoreFiles and MemStore each Store is uniquely identified by its columnFamily
 * @author ibra
 */
public class Store {

  private static final Log LOG = LogFactory.getLog(Store.class.getName());

  /**
   * HDFS instance to simulate read/write from HDFS
   * one HDFS object per Store, because 1 Store represents here Store in separate HBase cluster
   */
  private final HDFS hdfs;

  /**
   * compaction algorithm and compaction configuration for this Store
   */
  private final AbstractCompactor compactor;

  /**
   * cached value of read amplification
   */
  private long readAmp;

  /**
   * column family of this Store
   */
  private final int columnFamily;

  /**
   * memstore of this Store
   */
  private final MemStore memStore;

  /**
   * collection of store files of this Store
   */
  private final StoreFileCollection storeFiles;

  /**
   * queue for executing instruction that are coming from parallel threads
   */
  private final ExecuteQueue executeQueue;

  /**
   * creates and initializes Store
   * @param columnFamily - columnFamily of this Store
   */
  public Store(final int columnFamily) {
    this.hdfs = new HDFS();
    this.columnFamily = columnFamily;
    this.memStore = new MemStore();
    this.storeFiles = new StoreFileCollection();
    this.readAmp = 0;
    this.executeQueue = new ExecuteQueue();
    this.compactor = Simulator.INSTANCE.getCompactionConfiguration(columnFamily).getCompactor(
      this.hdfs, this);
  }

  /**
   * put instruction - put KeyValuePack - Collection of KeyValues
   */
  public void put() {
    this.executeQueue.put();
  }

  /**
   * invoke major compaction
   */
  public void forceMajorCompaction() {
    Store.this.executeQueue.forceMajorCompaction();
  }

  /**
   * do put KeyValuePack into this Store
   */
  private void doPut() {
    this.memStore.put();
    if (this.memStore.isFull()) {
      this.executeQueue.flush();
    }
  }

  /**
   * do compaction
   */
  private void doCompaction() {
    this.compactor.doCompaction(this.storeFiles);
  }

  /**
   * do force major compaction
   */
  private void doMajorCompaction() {
    this.compactor.forceMajorCompaction(this.storeFiles);
  }

  /**
   * do memstore flush
   * @param stream - stream to read/write from/to HDFS during flush
   */
  private void doFlush(final HDFSStream stream) {
    final StoreFile storeFile = this.memStore.flush(stream);
    this.storeFiles.add(storeFile);
    Simulator.INSTANCE.flushOccurred(this.columnFamily, storeFile.getBytesSize());
    this.readAmp = this.storeFiles.size();

    // new file added to storeFiles, so maybe compaction maybe needed
    this.executeQueue.compaction();
  }

  /**
   * do adding compacted storeFile to storeFiles
   * @param storeFile compacted storeFile
   * @param totalHdfsIO - total amount of bytes that were read/written from/to HDFS during this compaction
   */
  private void doAdd(final StoreFile storeFile, final long totalHdfsIO) {
    this.storeFiles.add(storeFile);
    Simulator.INSTANCE.compactionOccurred(this.columnFamily, totalHdfsIO);
    this.readAmp = this.storeFiles.size();

    // new file added to storeFiles, so maybe compaction maybe needed
    this.executeQueue.compaction();
  }

  /**
   * @return readAmplification in this Store
   */
  public double getReadAmplification() {
    return this.readAmp;// this.storeFiles.size();
  }

  /**
   * compaction was finished in BackgroundCompactor, so we need to add resulted compacted StoreFile
   * to storeFiles
   * @param compacted - compacted file resulted from BackgroundCompactor after finishing compaction
   * @param totalHdfsIO - total amount of bytes that were read/written from/to HDFS during this compaction 
   */
  public void compactionFinished(final StoreFile compacted, final long totalHdfsIO) {
    this.executeQueue.compactionFinished(compacted, totalHdfsIO);
  }

  /**
   * queue for executing instruction that are coming from parallel threads
   * @author ibra
   */
  private class ExecuteQueue {
    /**
     * queue of instructions types
     */
    private final Deque<StoreOperationType> deque = new LinkedBlockingDeque<StoreOperationType>();

    /**
     * queue of StoreFiles resulted from BackgroundCompactor that need to be added to
     * Store.storeFiles
     */
    private final Queue<StoreFile> compactedQueue = new LinkedBlockingQueue<StoreFile>();

    /**
     * queue of Longs - total HDFS Bytes IO done during this compaction
     */
    private final Queue<Long> totalHdfsIOQueue = new LinkedBlockingQueue<Long>();

    private ExecuteQueue() {
      /**
       * separate thread for logging
       */
      new Thread(new Runnable() {
        @Override
        public void run() {
          while (!Simulator.INSTANCE.isStopped()) {
            Helper.logSleep();
            // if deque size exeeds 1000 it means that load is too high
            if (ExecuteQueue.this.deque.size() >= 1000) {
              LOG.info("Store " + Store.this.columnFamily + " in queue = "
                  + ExecuteQueue.this.deque.size());
            }
          }
        }
      }).start();

      /**
       * thread for processing queue of instructions
       */
      new Thread(new Runnable() {
        @Override
        public void run() {
          final HDFSStream stream = new HDFSStream(Store.this.hdfs);

          while (!Simulator.INSTANCE.isStopped()) {
            Helper.sleep();
            while (!ExecuteQueue.this.deque.isEmpty()) {
              final StoreOperationType ev = ExecuteQueue.this.deque.poll();
              switch (ev) {
              case PUT:
                Store.this.doPut();
                break;
              case COMPACTION:
                Store.this.doCompaction();
                break;
              case MAJOR_COMPACTION:
                Store.this.doMajorCompaction();
                break;
              case FLUSH:
                Store.this.doFlush(stream);
                break;
              case COMPACTION_FINISHED:
                Store.this.doAdd(ExecuteQueue.this.compactedQueue.poll(),
                  ExecuteQueue.this.totalHdfsIOQueue.poll());
                break;
              }
            }
          }
        }
      }).start();
    }

    /**
     * put KeyValuePack instruction
     */
    private void put() {
      this.deque.add(StoreOperationType.PUT);
    }

    /**
     * minor compaction instruction
     */
    private void compaction() {
      this.deque.add(StoreOperationType.COMPACTION);
    }

    /**
     * major compaction instruction
     */
    private void forceMajorCompaction() {
      this.deque.add(StoreOperationType.MAJOR_COMPACTION);
    }

    /**
     * memstore flush instruction
     */
    private void flush() {
      this.deque.addFirst(StoreOperationType.FLUSH);
    }

    /**
     * compaction was finished in BackgroundCompactor, so we need to add resulted compacted
     * StoreFile to storeFiles
     * @param compacted - compacted file resulted from BackgroundCompactor after finishing
     *          compaction
     * @param totalHdfsIO - total amount of bytes that were read/written from/to HDFS during this compaction
     */
    private void compactionFinished(final StoreFile compacted, final long totalHdfsIO) {
      this.compactedQueue.add(compacted);
      this.totalHdfsIOQueue.add(totalHdfsIO);
      this.deque.addFirst(StoreOperationType.COMPACTION_FINISHED);
    }
  }

  /**
   * enum of instructions type
   * @author ibra
   */
  enum StoreOperationType {
    PUT, COMPACTION, MAJOR_COMPACTION, FLUSH, COMPACTION_FINISHED;
  }
}
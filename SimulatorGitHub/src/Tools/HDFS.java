package Tools;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import Model.Configuration;

/**
 * HDFS class helps to emulate delay of read/write from/to HDFS
 * This class is enum-based Singleton
 * 
 * this class allows parallel threads to read from HDFS in this way:
 * in any case overall read/write speed is bounded with
 * HDFS_READ_BYTES_PER_SECOND  - from Configuration
 * HDFS_WRITE_BYTES_PER_SECOND  - from Configuration
 * 
 * so when some thread tries to read file we read it here by 
 * blocks, so that HDFS throughput can be divided between several 
 * threads and one thread won't block others from accessing HDFS
 * 
 * @author ibra
 *
 */
public class HDFS {

  private static final Log LOG = LogFactory.getLog(HDFS.class.getName());

  /**
   * read/write block size
   */
  static final long BLOCK_SIZE = 128L * 1024L * 1024L * 1024L;

  /**
   * HDFSWaiter to wait for HDFS read
   */
  private HDFSWaiter reader;

  /**
   * HDFSWaiter to wait for HDFS write
   */
  private HDFSWaiter writer;

  /**
   * creates and initializes object
   */
  public HDFS() {
    this.reader = new HDFSWaiter();
    this.writer = new HDFSWaiter();
  }

  /**
   * calls synchronizedSleep - read BLOCK_SIZE bytes synchronously
   */
  void readRequest() {
    this.reader.synchronizedSleep(this.getReadFromHDFSDelay(BLOCK_SIZE));
  }

  /**
   * calls synchronizedSleep - write BLOCK_SIZE bytes synchronously
   */
  void writeRequest() {
    this.writer.synchronizedSleep(this.getWriteToHDFSDelay(BLOCK_SIZE));
  }

  /**
   * @param bytes amount of bytes to write to HDFS
   * @return amount of milliseconds that it will take to write bytes to HDFS
   */
  long getWriteToHDFSDelay(final long bytes) {
    return bytes
        * 1000L
        / (Configuration.INSTANCE.getHDFSWriteBytesPerSecond() * Configuration.INSTANCE
            .getxFaster());
  }

  /**
   * @param bytes amount of bytes to read to HDFS
   * @return amount of milliseconds that it will take to read bytes to HDFS
   */
  long getReadFromHDFSDelay(final long bytes) {
    return bytes
        * 1000L
        / (Configuration.INSTANCE.getHDFSReadbytesPerSecond() * Configuration.INSTANCE.getxFaster());
  }
}

class HDFSWaiter {
  /**
   * SYNCHRONIZED METHOD only one thread at a time can be inside this method. this fact makes HDFS
   * throughput bounded
   * @param time time to sleep
   */
  synchronized void synchronizedSleep(final long ms) {
    Helper.sleepTo(System.currentTimeMillis() + ms);
  }
}
package Tools;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * HDFSStream class - class to read/write data from HDFS within one thread.
 * makes easier to access to HDFS from parallel threads and to read/write with some bounded rate.
 * @author ibra
 *
 */
public class HDFSStream {

  private static final Log LOG = LogFactory.getLog(HDFSStream.class.getName());

  /**
   * HDFS object to read/write from HDFS
   */
  private final HDFS hdfs;

  /**
   * cached bytes to read.
   * So sometimes we need to read/write small files, so when we need to 
   * do that, actually we need to do sleep for nanoseconds, which
   * is very inaccurate, so that is what we do:
   * when some thread wants to read some amount of bytes we add it to cachedBytesToRead,
   * and when cachedBytesToRead reaches HDFS.BLOCK_SIZE (it takes about 30ms to read/write it)
   * we do actually wait reading/writing whole block, and not waiting when cachedBytesToRead 
   * is smaller that HDFS.BLOCK_SIZE
   */
  private long cachedBytesToRead;

  /**
   * creates and initializes object
   * @param hdfs - HDFS object to read/write from HDFS
   */
  public HDFSStream(final HDFS hdfs) {
    this.cachedBytesToRead = 0;
    this.hdfs = hdfs;
  }

  /**
   * read request
   * @param bytes - amount of bytes to read 
   */
  public void read(final long bytes) {
    this.cachedBytesToRead += bytes;
    while (this.cachedBytesToRead >= HDFS.BLOCK_SIZE) {
      this.cachedBytesToRead -= HDFS.BLOCK_SIZE;
      this.hdfs.readRequest();
    }
  }

  /**
   * write request
   * @param bytes - amount of bytes to write
   */
  public void write(final long bytes) {
    this.cachedBytesToRead += bytes;
    while (this.cachedBytesToRead >= HDFS.BLOCK_SIZE) {
      this.cachedBytesToRead -= HDFS.BLOCK_SIZE;
      this.hdfs.writeRequest();
    }
  }
}

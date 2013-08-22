package Model.HBaseElements;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import Model.HBaseElements.KeyValueDatas.KeyValueData;
import Tools.HDFSStream;

/**
 * StoreFile
 * @author ibra
 */
public class StoreFile {

  private static final Log LOG = LogFactory.getLog(StoreFile.class.getName());

  /**
   * data - Collection of KeyValues in this StoreFile
   */
  private final KeyValueData data;

  /**
   * creates and initializes empty StoreFile
   */
  public StoreFile() {
    this.data = KeyValueData.getKeyValueData();
  }

  /**
   * creates and initializes StoreFile
   * @param data - KeyValueData - to initialize StoreFile.data
   * @param stream - stream to read/write from/to HDFS during working with storeFiles
   */
  public StoreFile(final KeyValueData data, final HDFSStream stream) {
    data.compress();
    this.data = KeyValueData.getKeyValueData(data);
    stream.write(this.getBytesSize());
  }

  /**
   * merges two StoreFiles
   * @param other - storeFile to merge with
   * @param stream - stream to read/write from/to HDFS during working with storeFiles
   */
  public void mergeWith(final StoreFile other, final HDFSStream stream) {
    stream.read(other.getBytesSize());
    final long mergedBytes = this.data.mergeWith(other.data);
    stream.write(mergedBytes);
  }

  /**
   * @return size of this storeFile in bytes
   */
  public long getBytesSize() {
    return this.data.getBytesSize();
  }
}
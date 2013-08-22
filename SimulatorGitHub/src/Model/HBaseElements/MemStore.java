package Model.HBaseElements;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import Model.Configuration;
import Model.HBaseElements.KeyValueDatas.KeyValueData;
import Tools.HDFSStream;

/**
 * MemStore
 * @author ibra
 */
public class MemStore {

  private static final Log LOG = LogFactory.getLog(MemStore.class.getName());

  /**
   * data - Collection of KeyValues in this memStore
   */
  private final KeyValueData data;

  /**
   * creates and initializes object
   */
  public MemStore() {
    this.data = KeyValueData.getKeyValueData();
  }

  /**
   * puts KeyValuePack to this MemStore
   */
  public void put() {
    this.data.addKeyValuePack();
  }

  /**
   * @return if this MemStore is Full
   */
  public boolean isFull() {
    return this.getBytesSize() >= Configuration.INSTANCE.getMemstoreBytesSize();
  }

  /**
   * this method does flush of this MemStore
   * @param stream - stream to read/write from/to HDFS during flush
   * @return StoreFile created from this flush
   */
  public StoreFile flush(final HDFSStream stream) {
    final StoreFile storeFile = new StoreFile(this.data, stream);
    this.data.clear();
    return storeFile;
  }

  /**
   * @return size in bytes of written KeyValues in this MemStore
   */
  public long getBytesSize() {
    return this.data.getBytesSize();
  }
}
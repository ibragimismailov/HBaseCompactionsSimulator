package Model.HBaseElements.KeyValueDatas;

import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import Model.Configuration;

/**
 * KeyValueData - emulates collection of KeyValues
 * @author ibra
 */
public abstract class KeyValueData {

  private static final Log LOG = LogFactory.getLog(KeyValueData.class.getName());

  /**
   * total size of this KeyValue collection
   */
  protected long bytesSize;

  /**
   * for each KeyValuePack we store its create time to know when its TLL expires
   */
  protected List<Long> keyValuePacksCreateTime;

  /**
   * for each KeyValuePack we store its size
   */
  protected List<Long> keyValuePackByteSizes;

  /**
   * creates empty KeyValueData
   */
  public static KeyValueData getKeyValueData() {
    if (Configuration.INSTANCE.isKeyValuesTTLEnabled()) {
      return new KeyValueDataWithTTL();
    } else {
      return new KeyValueDataWithoutTTL();
    }
  }

  /**
   * creates KeyValueData as clone as keyValueData
   */
  public static KeyValueData getKeyValueData(final KeyValueData keyValueData) {
    if (Configuration.INSTANCE.isKeyValuesTTLEnabled()) {
      return new KeyValueDataWithTTL(keyValueData);
    } else {
      return new KeyValueDataWithoutTTL(keyValueData);
    }
  }

  /**
   * adds KeyValuePack to KeyValueData
   */
  public abstract void addKeyValuePack();

  /**
   * compressing data before flush
   */
  public abstract void compress();

  /**
   * merges this KeyValueData with other method deletes KeyValuePacks that has expired TTL
   * @param other - KeyValueData this to be merged with
   * @return amount of bytes that was merged with this from other
   */
  public abstract long mergeWith(final KeyValueData other);

  /**
   * clear this KeyValueData
   */
  public abstract void clear();

  /**
   * @return byte size of this KeyValueData
   */
  public long getBytesSize() {
    return this.bytesSize;
  }
}

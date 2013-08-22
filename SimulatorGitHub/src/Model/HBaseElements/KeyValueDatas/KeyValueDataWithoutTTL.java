package Model.HBaseElements.KeyValueDatas;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import Model.Configuration;
import Tools.RandomGenerator;

/**
 * KeyValueData - emulates collection of KeyValues
 * @author ibra
 */
public class KeyValueDataWithoutTTL extends KeyValueData {

  private static final Log LOG = LogFactory.getLog(KeyValueDataWithoutTTL.class.getName());

  /**
   * creates empty KeyValueData
   */
  public KeyValueDataWithoutTTL() {
    this.bytesSize = 0;
  }

  /**
   * creates KeyValueData as clone as keyValueData
   */
  public KeyValueDataWithoutTTL(final KeyValueData keyValueData) {
    this.bytesSize = keyValueData.bytesSize;
  }

  /**
   * adds KeyValuePack to KeyValueData
   */
  @Override
  public void addKeyValuePack() {
    this.bytesSize += RandomGenerator.getKeyValuePackBytesSize();
  }

  /**
   * compressing data before flush
   */
  @Override
  public void compress() {
    this.bytesSize /= Configuration.INSTANCE.COMPRESSION_RATIO;
  }

  /**
   * merges this KeyValueData with other method deletes KeyValuePacks that has expired TTL
   * @param other - KeyValueData this to be merged with
   * @return amount of bytes that was merged with this from other
   */
  @Override
  public long mergeWith(final KeyValueData other) {
    this.bytesSize += other.bytesSize;
    return other.getBytesSize();
  }

  /**
   * clear this KeyValueData
   */
  @Override
  public void clear() {
    this.bytesSize = 0;
  }
}

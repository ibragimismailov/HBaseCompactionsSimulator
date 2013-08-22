package Model.HBaseElements.KeyValueDatas;

import java.util.ArrayList;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import Model.Configuration;
import Tools.RandomGenerator;

/**
 * KeyValueData - emulates collection of KeyValues
 * @author ibra
 */
public class KeyValueDataWithTTL extends KeyValueData {

  private static final Log LOG = LogFactory.getLog(KeyValueDataWithTTL.class.getName());

  /**
   * creates empty KeyValueData
   */
  KeyValueDataWithTTL() {
    this.bytesSize = 0;
    this.keyValuePacksCreateTime = new ArrayList<Long>();
    this.keyValuePackByteSizes = new ArrayList<Long>();
  }

  /**
   * creates KeyValueData as clone as keyValueData
   */
  KeyValueDataWithTTL(final KeyValueData keyValueData) {
    this.bytesSize = keyValueData.bytesSize;
    this.keyValuePacksCreateTime = new ArrayList<Long>();
    this.keyValuePackByteSizes = new ArrayList<Long>();
    this.keyValuePacksCreateTime.addAll(keyValueData.keyValuePacksCreateTime);
    this.keyValuePackByteSizes.addAll(keyValueData.keyValuePackByteSizes);
  }

  /**
   * adds KeyValuePack to KeyValueData
   */
  @Override
  public void addKeyValuePack() {
    final long kvpSize = RandomGenerator.getKeyValuePackBytesSize();
    this.bytesSize += kvpSize;
    this.keyValuePackByteSizes.add(kvpSize);
    this.keyValuePacksCreateTime.add(System.currentTimeMillis());
  }

  /**
   * compressing data before flush
   */
  @Override
  public void compress() {
    this.bytesSize = 0;
    for (int i = 0; i < this.keyValuePackByteSizes.size(); i++) {
      this.keyValuePackByteSizes.set(i, this.keyValuePackByteSizes.get(i)
          / Configuration.INSTANCE.COMPRESSION_RATIO);
      this.bytesSize += this.keyValuePackByteSizes.get(i);
    }
  }

  /**
   * merges this KeyValueData with other method deletes KeyValuePacks that has expired TTL
   * @param other - KeyValueData this to be merged with
   * @return amount of bytes that was merged with this from other
   */
  @Override
  public long mergeWith(final KeyValueData other) {
    long mergedBytes = 0;
    final long curTime = System.currentTimeMillis();
    for (int i = 0; i < other.keyValuePacksCreateTime.size(); i++) {
      final long createdTime = other.keyValuePacksCreateTime.get(i);
      final long byteSize = other.keyValuePackByteSizes.get(i);

      // if TTL is expired we don't add this keyValuePack to this
      if ((curTime - createdTime) * Configuration.INSTANCE.getxFaster() >= RandomGenerator
          .getKeyValueTTL()) {
        continue;
      }

      mergedBytes += byteSize;
      this.bytesSize += byteSize;
      this.keyValuePackByteSizes.add(byteSize);
      this.keyValuePacksCreateTime.add(createdTime);
    }
    return mergedBytes;
  }

  /**
   * clear this KeyValueData
   */
  @Override
  public void clear() {
    this.bytesSize = 0;
    this.keyValuePackByteSizes.clear();
    this.keyValuePacksCreateTime.clear();
  }
}

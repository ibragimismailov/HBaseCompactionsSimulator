package Model;

import java.util.Map;
import java.util.TreeMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import GUI.ConfigurationFrames.AbstractConfigurationFrame;
import GUI.ConfigurationFrames.ConfigurationFrame;

/**
 * Class Configuration describes general configuration of HBase (shown in gui in ConfigurationFrame on left side)
 * This class is Singleton
 * configuration values are changed by ConfigurationFrame
 * 
 * To add new configuration value: 
 * 1) add private field with public getter and setter 
 * 2) add it to fillGetFields and fillSetFields
 * @author ibra
 */
public class Configuration extends AbstractConfiguration {

  private static final Log LOG = LogFactory.getLog(Configuration.class.getName());

  /**
   * instance of Singleton
   */
  public static final Configuration INSTANCE = new Configuration();

  private Configuration() {
  }

  /**
   * milliseconds per day
   */
  public static final long MS_PER_DAY = 24L * 3600L * 1000L;

  /**
   * time gap between consecutive logs
   */
  public final long SLEEP_LOG_TIME = 1000;

  /**
   * compression ratio - when memstore flush occurs we take memstore, compress it and write to
   * storeFile, so this storeFile size = memstore size / compression ratio
   */
  public final long COMPRESSION_RATIO = 10;

  /**
   * time gap between consecutive chart updates
   */
  public final long CHARTS_UPDATE_GAP = 1000;

  /**
   * maximal amount of compaction algorithms to simulate
   */
  public final int MAX_COMPACTION_ALGOS_COUNT = 10;

  /**
   * Gap between flushes(ms) - time gap between two consecutive flushes in one store
   * So if Region contains N Stores, flush gap in this Store is about flushGap/N
   */
  private long flushGap = 40000;

  /**
   * Memstore flush size(bytes) - size of MemStore
   */
  private long memstoreBytesSize = 100000000; // hbase.hregion.memstore.flush.size

  /**
   * Compaction algos count - amount of compaction algorithms you want to test
   */
  private long compactionAlgosCount = 1;

  /**
   * KeyValue TTL(ms) - average TTL for KeyValues in HBase
   */
  private long keyValueTTL = 10L * Configuration.MS_PER_DAY;

  /**
   * KeyValue TTL jitter - jitter used for getting KeyValue TTL.
   * Watch RandomGenerator.getJitteredValue for clarifications
   */
  private double keyValueTTLJitter = 0.3;

  /**
   * KeyValue bytes size - average size of KeyValue in HBase
   */
  private long keyValueByteSize = 100;

  /**
   * KeyValue bytes size jitter - jitter used for getting KeyValue size.
   * Watch RandomGenerator.getJitteredValue for clarifications
   */
  private double keyValueByteSizeJitter = 0.3;

  /**
   * Read bytes per second to HDFS - read rate throughput from HDFS
   */
  private long HDFSReadBytesPerSecond = 50L * 1024L * 1024L;

  /**
   * Write bytes per second from HDFS - write rate throughput to HDFS
   */
  private long HDFSWriteBytesPerSecond = 50L * 1024L * 1024L;

  /**
   * xFaster - speed coefficient. makes simulator simulate everything xFaster times faster
   * for example default value here is 10000.
   * That means that Simulator simulates 1 year of HBase work in 1/10000 years (it is 50 minutes) 
   * this value is recommended to be the same as flushGap/(compactionAlgosCount*4) or less.
   */
  private long xFaster = 10000;

  /**
   * size of KeyValuePack. KeyValuePack is some amount of KeyValues that we send to HBase as one
   * object during put, considering as we are sending a bunch of KeyValues
   */
  private long kvsPerPut = this.memstoreBytesSize / (3 * this.keyValueByteSize) + 1;

  /**
   * is TTLs for KeyValues enabled (for example for Specific2 cluster TTLs are N days, and for Specific1 configuration there is no TTLs
   * we need this configuration, because storing TTL information about all keyValues without deleting them when their's TTL is expired is too expensive 
   */
  public boolean isKeyValuesTTLEnabled() {
    return this.keyValueTTL != -1;
  }

  /**
   * method return ConfigurationFrame for changing this configuration
   * @param title - main title for ConfigurationFrame
   * @param getFields - get fields map of compaction configuration
   * @param setFields - get fields map of compaction configuration
   */
  @Override
  protected AbstractConfigurationFrame getConfigurationFrame(final String title,
      final Map<String, GetMethod> getFields, final Map<String, SetMethod> setFields) {
    return new ConfigurationFrame(title, getFields, setFields);
  }

  /**
   * fill getFields map
   */
  @Override
  protected Map<String, GetMethod> fillGetFields() {// @formatter:off
    final Map<String, GetMethod> res = new TreeMap<String, GetMethod>();
    res.put("Gap between flushes(ms)",         new GetMethod() { @Override public String get() {return Long  .toString(Configuration.this.getFlushGap               ());} });
    res.put("Memstore flush size(bytes)",      new GetMethod() { @Override public String get() {return Long  .toString(Configuration.this.getMemstoreBytesSize      ());} });
    res.put("Compaction algos count",          new GetMethod() { @Override public String get() {return Long  .toString(Configuration.this.getCompactionAlgosCount   ());} });
    res.put("KeyValue TTL(ms)",                new GetMethod() { @Override public String get() {return Long  .toString(Configuration.this.getKeyValueTTL            ());} });
    res.put("KeyValue TTL jitter",             new GetMethod() { @Override public String get() {return Double.toString(Configuration.this.getKeyValueTTLJitter      ());} });
    res.put("KeyValue size(bytes)",            new GetMethod() { @Override public String get() {return Long  .toString(Configuration.this.getKeyValueByteSize       ());} });
    res.put("KeyValue size jitter",            new GetMethod() { @Override public String get() {return Double.toString(Configuration.this.getKeyValueByteSizeJitter ());} });
    res.put("Read bytes per second from HDFS", new GetMethod() { @Override public String get() {return Long  .toString(Configuration.this.getHDFSReadbytesPerSecond ());} });
    res.put("Write bytes per second to HDFS",  new GetMethod() { @Override public String get() {return Long  .toString(Configuration.this.getHDFSWriteBytesPerSecond());} });
    res.put("xFaster",                         new GetMethod() { @Override public String get() {return Long  .toString(Configuration.this.getxFaster                ());} });
    res.put("KeyValues count per put",         new GetMethod() { @Override public String get() {return Long  .toString(Configuration.this.getKvsPerPut              ());} });
    return res;
  }// @formatter:on

  /**
   * fill setFields map
   */
  @Override
  protected Map<String, SetMethod> fillSetFields() {// @formatter:off
    final Map<String, SetMethod> res = new TreeMap<String, SetMethod>();
    res.put("Gap between flushes(ms)",         new SetMethod() { @Override public void set(String value) {Configuration.this.setFlushGap               (value);} });
    res.put("Memstore flush size(bytes)",      new SetMethod() { @Override public void set(String value) {Configuration.this.setMemstoreBytesSize      (value);} });
    res.put("Compaction algos count",          new SetMethod() { @Override public void set(String value) {Configuration.this.setCompactionAlgosCount   (value);} });
    res.put("KeyValue TTL(ms)",                new SetMethod() { @Override public void set(String value) {Configuration.this.setKeyValueTTL            (value);} });
    res.put("KeyValue TTL jitter",             new SetMethod() { @Override public void set(String value) {Configuration.this.setKeyValueTTLJitter      (value);} });
    res.put("KeyValue size(bytes)",            new SetMethod() { @Override public void set(String value) {Configuration.this.setKeyValueByteSize       (value);} });
    res.put("KeyValue size jitter",            new SetMethod() { @Override public void set(String value) {Configuration.this.setKeyValueByteSizeJitter (value);} });
    res.put("Read bytes per second from HDFS", new SetMethod() { @Override public void set(String value) {Configuration.this.setHDFSReadBytesPerSecond (value);} });
    res.put("Write bytes per second to HDFS",  new SetMethod() { @Override public void set(String value) {Configuration.this.setHDFSWriteBytesPerSecond(value);} });
    res.put("xFaster",                         new SetMethod() { @Override public void set(String value) {Configuration.this.setxFaster                (value);} });
    res.put("KeyValues count per put",         new SetMethod() { @Override public void set(String value) {Configuration.this.setKvsPerPut              (value);} });
    return res;
  }// @formatter:on

  // ===================================================================
  // =============================<GETTERS>=============================
  // ===================================================================
  public long getFlushGap() {
    return this.flushGap;
  }

  public long getMemstoreBytesSize() {
    return this.memstoreBytesSize;
  }

  public long getCompactionAlgosCount() {
    return this.compactionAlgosCount;
  }

  public long getKeyValueTTL() {
    return this.keyValueTTL;
  }

  public double getKeyValueTTLJitter() {
    return this.keyValueTTLJitter;
  }

  public long getKeyValueByteSize() {
    return this.keyValueByteSize;
  }

  public double getKeyValueByteSizeJitter() {
    return this.keyValueByteSizeJitter;
  }

  public long getHDFSReadbytesPerSecond() {
    return this.HDFSReadBytesPerSecond;
  }

  public long getHDFSWriteBytesPerSecond() {
    return this.HDFSWriteBytesPerSecond;
  }

  public long getxFaster() {
    return this.xFaster;
  }

  public long getKvsPerPut() {
    return this.kvsPerPut;
  }

  // ===================================================================
  // ============================</GETTERS>=============================
  // ===================================================================

  // ===================================================================
  // =============================<SETTERS>=============================
  // ===================================================================
  public void setFlushGap(String flushGap) {
    this.flushGap = Long.parseLong(flushGap);
  }

  public void setMemstoreBytesSize(String memstoreBytesSize) {
    this.memstoreBytesSize = Long.parseLong(memstoreBytesSize);
  }

  public void setCompactionAlgosCount(String compactionAlgosCount) {
    this.compactionAlgosCount = Long.parseLong(compactionAlgosCount);
  }

  public void setKeyValueTTL(String keyValueTTL) {
    this.keyValueTTL = Long.parseLong(keyValueTTL);
  }

  public void setKeyValueTTLJitter(String keyValueTTLJitter) {
    this.keyValueTTLJitter = Double.parseDouble(keyValueTTLJitter);
  }

  public void setKeyValueByteSize(String keyValueByteSize) {
    this.keyValueByteSize = Long.parseLong(keyValueByteSize);
  }

  public void setKeyValueByteSizeJitter(String keyValueByteSizeJitter) {
    this.keyValueByteSizeJitter = Double.parseDouble(keyValueByteSizeJitter);
  }

  public void setHDFSReadBytesPerSecond(String HDFSReadBytesPerSecond) {
    this.HDFSReadBytesPerSecond = Long.parseLong(HDFSReadBytesPerSecond);
  }

  public void setHDFSWriteBytesPerSecond(String hDFSWriteBytesPerSecond) {
    this.HDFSWriteBytesPerSecond = Long.parseLong(hDFSWriteBytesPerSecond);
  }

  public void setxFaster(String xFaster) {
    this.xFaster = Long.parseLong(xFaster);
  }

  public void setKvsPerPut(String kvsPerPut) {
    this.kvsPerPut = Long.parseLong(kvsPerPut);
  }
  // ===================================================================
  // ============================</SETTERS>=============================
  // ===================================================================
}

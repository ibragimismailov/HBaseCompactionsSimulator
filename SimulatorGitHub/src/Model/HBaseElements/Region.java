package Model.HBaseElements;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import Model.Simulator;

/**
 * Region - contains collection of Stores - each Store has unique columnFamily
 * @author ibra
 */
public class Region {

  private static final Log LOG = LogFactory.getLog(Region.class.getName());

  /**
   * stores of this region
   * this list uses columnFamily as index to Store (since each Store has unique columnFamily)
   */
  private final List<Store> stores;

  /**
   * creates and initializes object
   * @param storesCount - amount of stores in this region
   */
  public Region(final int storesCount) {
    this.stores = new ArrayList<Store>();
    for (int i = 0; i < storesCount; i++) {
      this.stores.add(new Store(i));
    }
  }

  /**
   * put KeyValuePack to Region
   * @param columnFamily - columnFamily of keyVlaues in KeyValuePack
   */
  public void put(final int columnFamily) {
    this.stores.get(columnFamily).put();
  }

  /**
   * @return List<Double> - list of read amplification for each Store in this Region
   *         this list uses columnFamily as index to Store (since each Store has unique columnFamily)
   */
  public List<Double> getReadAmplification() {
    final List<Double> res = new ArrayList<Double>();
    for (int i = 0; i < this.stores.size(); i++) {
      res.add(this.stores.get(i).getReadAmplification());
    }
    return res;
  }

  /**
   * @return list of titles of each Store.
   *         Store's title is its columnFamily and compactionAlgorithmType, 
   *         this list uses columnFamily as index to Store (since each Store has unique columnFamily)
   */
  public List<String> getStoreTitles() {
    List<String> res = new ArrayList<String>();
    for (int i = 0; i < this.stores.size(); i++) {
      res.add(i + "(" + Simulator.INSTANCE.getCompactionConfiguration(i).getTitle() + ")");
    }
    return res;
  }
}

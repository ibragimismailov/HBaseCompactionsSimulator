package Model.HBaseElements;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * collection of storeFiles sorted by decreasing byte size
 * @author ibra
 */
public class StoreFileCollection implements Iterable<StoreFile> {

  private static final Log LOG = LogFactory.getLog(StoreFileCollection.class.getName());

  /**
   * StoreFileCollection is wrap over this list
   */
  private final List<StoreFile> storeFiles;

  /**
   * creates empty collection
   */
  public StoreFileCollection() {
    this.storeFiles = new ArrayList<StoreFile>();
  }

  /**
   * creates collection as clone of storeFileCollection
   */
  public StoreFileCollection(final StoreFileCollection storeFileCollection) {
    this.storeFiles = new ArrayList<StoreFile>(storeFileCollection.storeFiles);
  }

  /**
   * creates collection as collection of StoreFiles from storeFileList
   */
  private StoreFileCollection(final List<StoreFile> storeFileList) {
    this.storeFiles = new ArrayList<StoreFile>(storeFileList);
  }

  /**
   * adds storeFile to collection
   * @param storeFile - storeFile to be added
   */
  synchronized public void add(final StoreFile storeFile) {
    // if collection is empty or size of storeFile is bigger than any other in this collection, then
    // we insert it the beginning
    if (this.storeFiles.isEmpty()
        || storeFile.getBytesSize() > this.storeFiles.get(0).getBytesSize()) {
      this.storeFiles.add(0, storeFile);
    } else {
      // else find place to insert so that after insert collection to stay sorted
      for (int i = this.storeFiles.size() - 1; i >= 0; i--) {
        if (this.storeFiles.get(i).getBytesSize() >= storeFile.getBytesSize()) {
          this.storeFiles.add(i + 1, storeFile);
          break;
        }
      }
    }
  }

  /**
   * adds all elements from this storeFileCollection
   * @param storeFileCollection - collection to be added to this
   */
  synchronized public void addAll(final StoreFileCollection storeFileCollection) {
    for (StoreFile storeFile : storeFileCollection) {
      this.add(storeFile);
    }
  }

  /**
   * @param index index of storeFile to be returned
   * @return storeFile in [index] in this collection
   */
  synchronized public StoreFile get(final int index) {
    return this.storeFiles.get(index);
  }

  /**
   * removes specified element from the collection
   * @param storeFile - element to be removed from this collection
   */
  synchronized public void remove(final StoreFile storeFile) {
    this.storeFiles.remove(storeFile);
  }

  /**
   * removes all elements from this collection
   */
  synchronized public void clear() {
    this.storeFiles.clear();
  }

  /**
   * @param storeFile - storeFile to search
   * @return if collection contains this element
   */
  synchronized public boolean contains(final StoreFile storeFile) {
    return this.storeFiles.contains(storeFile);
  }

  /**
   * @return amount of elements in this collection
   */
  synchronized public int size() {
    return this.storeFiles.size();
  }

  /**
   * @return if collection is empty
   */
  synchronized public boolean isEmpty() {
    return this.storeFiles.isEmpty();
  }

  @Override
  public Iterator<StoreFile> iterator() {
    return this.storeFiles.iterator();
  }

  /**
   * @param fromIndex - begin index of sublist to be returned
   * @param toIndex - index after last index of sublist to be returned
   * @return sublist of this collection of range [fromIndex, toIndex)
   */
  synchronized public StoreFileCollection subList(final int fromIndex, final int toIndex) {
    return new StoreFileCollection(this.storeFiles.subList(fromIndex, toIndex));
  }
}

package GUI.Charts;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jfree.ui.RefineryUtilities;

import Model.Configuration;

/**
 * AdvancedTimeChart - TimeChart, that plots average graphs.
 * Current value of average graph is average value of last avgCount values given to update method
 * 
 * This class is used for plotting Read Amplification Factor(RAF) graphs.
 * We use this class for plotting RAF because RAF value changes very frequently and 
 * it is hard to compare just two RAF graphs without seeing its averages.
 * @author ibra
 */
public class AdvancedTimeChart extends TimeChart {

  private static final Log LOG = LogFactory.getLog(AdvancedTimeChart.class.getName());

  /**
   * stat contains last avgCount values of each original graph so Queue of doubles is last avgCount values
   */
  private final List<Queue<Double>> stat;

  /**
   * avgCount - amount of values we store for each graph to calculate average graph
   * configured in such way, that chart show average value for last 100 days
   */
  private long getAvgCount() {
    return 100L * Configuration.MS_PER_DAY
        / (Configuration.INSTANCE.CHARTS_UPDATE_GAP * Configuration.INSTANCE.getxFaster());
  }

  /**
   * creates AdvancedTimeChart object
   * @param mainTitle main title of chart
   * @param xTitle x-axis title
   * @param yTitle y-axis title
   * @param titles titles of each graph
   */
  protected AdvancedTimeChart(final String mainTitle, final String xTitle, final String yTitle,
      final String... titles) {
    super(mainTitle, xTitle, yTitle, titles);

    this.stat = new ArrayList<Queue<Double>>();
    for (String title : titles) {
      this.stat.add(new LinkedList<Double>());
    }
  }

  /**
   * Updates chart with new values
   * @param time - Time (x-axis value) of current update
   * @param values - contains new values for graphs value
   */
  public void update(final long time, final List<Double> values) {
    // update info in stat
    // and add points to original graphs and average graphs
    for (int i = 0; i < values.size(); i++) {
      // add point to original graph
      final double value = values.get(i);

      // if we already have at least avgCount of them
      // remove old value from stat
      while (this.stat.get(i).size() >= this.getAvgCount()) {
        this.stat.get(i).poll();
      }

      // add new value to stat
      this.stat.get(i).add(value);

      // calculate average value
      double avgValue = 0;
      for (Double v : this.stat.get(i)) {
        avgValue += v;
      }
      avgValue /= this.stat.get(i).size();

      // add point to average graph
      this.addPointToSeries(i, time, avgValue);
    }
  }

  /**
   * create, make visible, configure and return new instance of AdvancedTimeChart
   * @param mainTitle main title of chart
   * @param xTitle title of x-axis
   * @param yTitle title of y-axis
   * @param titles titles of graphs
   * @return instance of AdvancedTimeChart
   */
  public static AdvancedTimeChart go(final String mainTitle, final String xTitle,
      final String yTitle, final List<String> titles) {
    final String[] ts = new String[titles.size()];
    for (int i = 0; i < titles.size(); i++) {
      ts[i] = "avg-" + titles.get(i);
    }

    final AdvancedTimeChart chart = new AdvancedTimeChart(mainTitle, xTitle, yTitle, ts);
    chart.pack();
    RefineryUtilities.centerFrameOnScreen(chart);
    chart.setVisible(true);

    return chart;
  }
}

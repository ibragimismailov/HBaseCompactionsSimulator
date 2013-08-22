package GUI.Charts;

import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jfree.ui.RefineryUtilities;

/**
 * Simple TimeChart class.
 * Everything it does - just plots graphs and updates them with values given to update method
 * 
 * This class is used for plotting Write Amplification Factor graphs 
 * @author ibra
 */
public class SimpleTimeChart extends TimeChart {

  private static final Log LOG = LogFactory.getLog(SimpleTimeChart.class.getName());

  /**
   * creates SimpleTimeChart object
   * @param mainTitle - main title of chart
   * @param xTitle - x-axis title
   * @param yTitle - y-axis title
   * @param titles - titles of each graph
   */
  protected SimpleTimeChart(final String mainTitle, final String xTitle, final String yTitle,
      final String... titles) {
    super(mainTitle, xTitle, yTitle, titles);
  }

  /**
   * Updates graphs with new value (add new point for each graph)
   * @param time - Time (x-axis value) of current update
   * @param values - list of new values of graphs
   */
  public void update(final long time, final List<Double> values) {
    for (int i = 0; i < this.getGraphsCount(); i++) {
      this.addPointToSeries(i, time, values.get(i));
    }
  }

  /**
   * create, make visible, configure and return new instance of SimpleTimeChart
   * @param mainTitle - main title of chart
   * @param xTitle - title of x-axis
   * @param yTitle - title of y-axis
   * @param titles - titles of graphs
   * @return instance of SimpleTimeChart
   */
  public static SimpleTimeChart go(final String mainTitle, final String xTitle,
      final String yTitle, final List<String> titles) {
    final String[] ts = new String[titles.size()];
    for (int i = 0; i < titles.size(); i++) {
      ts[i] = titles.get(i);
    }

    final SimpleTimeChart chart = new SimpleTimeChart(mainTitle, xTitle, yTitle, ts);
    chart.pack();
    RefineryUtilities.centerFrameOnScreen(chart);
    chart.setVisible(true);

    return chart;
  }
}

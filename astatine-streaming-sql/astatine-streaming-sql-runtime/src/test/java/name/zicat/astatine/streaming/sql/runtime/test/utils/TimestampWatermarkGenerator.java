package name.zicat.astatine.streaming.sql.runtime.test.utils;

import org.apache.flink.api.common.eventtime.*;
import org.apache.flink.table.data.RowData;

/** TimestampWatermarkGenerator. */
public class TimestampWatermarkGenerator implements WatermarkGenerator<RowData> {

  private final int index;
  private final int latency;

  public TimestampWatermarkGenerator(int index, int latency) {
    this.index = index;
    this.latency = latency;
  }

  public TimestampWatermarkGenerator(int index) {
    this(index, 0);
  }

  @Override
  public void onEvent(RowData o, long l, WatermarkOutput watermarkOutput) {
    watermarkOutput.emitWatermark(
        new Watermark(o.getTimestamp(index, 3).getMillisecond() + latency));
  }

  @Override
  public void onPeriodicEmit(WatermarkOutput watermarkOutput) {}

  public static WatermarkStrategy<RowData> create(int index) {
    return create(index, 0);
  }

  public static WatermarkStrategy<RowData> create(int index, int latency) {
    return context -> new TimestampWatermarkGenerator(index, latency);
  }
}

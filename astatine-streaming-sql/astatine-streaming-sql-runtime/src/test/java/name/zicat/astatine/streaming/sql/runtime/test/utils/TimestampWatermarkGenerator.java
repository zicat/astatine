package name.zicat.astatine.streaming.sql.runtime.test.utils;

import org.apache.flink.api.common.eventtime.*;
import org.apache.flink.table.data.RowData;

/**
 * TimestampWatermarkGenerator.
 */
public class TimestampWatermarkGenerator implements WatermarkGenerator<RowData> {

    private final int index;

    public TimestampWatermarkGenerator(int index) {
        this.index = index;
    }

    @Override
    public void onEvent(RowData o, long l, WatermarkOutput watermarkOutput) {
    watermarkOutput.emitWatermark(new Watermark(o.getTimestamp(index, 3).getMillisecond()));
    }

    @Override
    public void onPeriodicEmit(WatermarkOutput watermarkOutput) {

    }

    public static WatermarkStrategy<RowData> create(int index) {
        return context -> new TimestampWatermarkGenerator(index);
    }
}

package name.zicat.astatine.streaming.sql.parser.function;

import name.zicat.astatine.streaming.sql.parser.transform.TransformContext;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStream;

/**
 * WatermarkFunctionFactory.
 *
 * @param <T>
 */
public interface WatermarkFunctionFactory<T>
    extends OneTransformFunctionFactory<DataStream<T>, T, T> {

  /**
   * create watermark strategy.
   *
   * @param context context
   * @return filter function.
   */
  WatermarkStrategy<T> createWatermarkStrategy(TransformContext context);

  @Override
  default DataStream<T> transform(TransformContext context, DataStream<T> stream) {
    return stream.assignTimestampsAndWatermarks(createWatermarkStrategy(context));
  }
}

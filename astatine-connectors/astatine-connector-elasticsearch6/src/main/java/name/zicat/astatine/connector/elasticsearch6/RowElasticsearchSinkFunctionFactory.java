package name.zicat.astatine.connector.elasticsearch6;

import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.connector.format.EncodingFormat;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.data.RowData;

import java.time.ZoneId;
import java.util.ArrayList;
import java.util.ServiceLoader;
import java.util.StringJoiner;

@SuppressWarnings("deprecation")
public interface RowElasticsearchSinkFunctionFactory {

  String identity();

  RowElasticsearchSinkFunction create(
      EncodingFormat<SerializationSchema<RowData>> format,
      Elasticsearch6Configuration config,
      TableSchema schema,
      ZoneId localTimeZoneId,
      Elasticsearch6DynamicSink.ElasticSearchBuilderProvider builderProvider,
      DynamicTableSink.Context context);

  static RowElasticsearchSinkFunctionFactory findFactory(String identity) {
    final ServiceLoader<RowElasticsearchSinkFunctionFactory> loader =
        ServiceLoader.load(RowElasticsearchSinkFunctionFactory.class);
    final var result = new ArrayList<RowElasticsearchSinkFunctionFactory>();
    for (RowElasticsearchSinkFunctionFactory factory : loader) {
      if (factory.identity().equals(identity)) {
        result.add(factory);
      }
    }
    if (result.isEmpty()) {
      throw new RuntimeException("identity " + identity + " not found");
    }
    if (result.size() > 1) {
      final var join = new StringJoiner(",");
      result.forEach(v -> join.add(v.getClass().getName()));
      throw new RuntimeException("identity " + identity + " found multi implement " + join);
    }
    return result.get(0);
  }

  /**
   * create routing config.
   *
   * @param config config
   * @param schema schema
   * @param localTimeZoneId localTimeZoneId
   * @return IndexGenerator
   */
  default IndexGenerator createRoutingGenerator(
      Elasticsearch6Configuration config, TableSchema schema, ZoneId localTimeZoneId) {
    return config.getRouting() == null
        ? new IndexGenerator.NullIndexGenerator()
        : IndexGeneratorFactory.createIndexGenerator(config.getRouting(), schema, localTimeZoneId);
  }

  /**
   * create index config.
   *
   * @param config config
   * @param schema schema
   * @param localTimeZoneId localTimeZoneId
   * @return IndexGenerator
   */
  default IndexGenerator createIndexGenerator(
      Elasticsearch6Configuration config, TableSchema schema, ZoneId localTimeZoneId) {
    return IndexGeneratorFactory.createIndexGenerator(config.getIndex(), schema, localTimeZoneId);
  }
}

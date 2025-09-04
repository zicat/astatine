package name.zicat.astatine.connector.doris.table;

import java.time.Duration;

import name.zicat.astatine.connector.doris.model.GroupCommitMode;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.ReadableConfig;

/** DorisConfigOptions. */
public class DorisConfigOptions {
  protected static final ConfigOption<String> FENODES =
      ConfigOptions.key("fenodes")
          .stringType()
          .noDefaultValue()
          .withDescription("doris fe http address.");
  protected static final ConfigOption<String> TABLE_IDENTIFIER =
      ConfigOptions.key("table.identifier")
          .stringType()
          .noDefaultValue()
          .withDescription("the jdbc table name.");
  protected static final ConfigOption<String> USERNAME =
      ConfigOptions.key("username")
          .stringType()
          .noDefaultValue()
          .withDescription("the jdbc user name.");
  protected static final ConfigOption<String> PASSWORD =
      ConfigOptions.key("password")
          .stringType()
          .noDefaultValue()
          .withDescription("the jdbc password.");

  protected static final ConfigOption<Duration> CONNECTION_TIMEOUT =
      ConfigOptions.key("connection.timeout").durationType().defaultValue(Duration.ofSeconds(15));

  protected static final ConfigOption<Duration> SOCKET_TIMEOUT =
      ConfigOptions.key("socket.timeout").durationType().defaultValue(Duration.ofSeconds(5));

  protected static final ConfigOption<Integer> SINK_RETRY_TIMES =
      ConfigOptions.key("retry.times")
          .intType()
          .defaultValue(3)
          .withDescription("the max retry times if writing records to database failed.");

  protected static final ConfigOption<Duration> SINK_RETRY_INTERVAL =
      ConfigOptions.key("retry.interval")
          .durationType()
          .defaultValue(Duration.ofSeconds(1))
          .withDescription("the interval time of retry if writing records to database failed.");

  protected static final ConfigOption<Integer> SINK_BUFFER_FLUSH_MAX_BYTES =
      ConfigOptions.key("sink.batch.bytes")
          .intType()
          .defaultValue(8388608)
          .withDescription(
              "the flush max bytes (includes all append, upsert and delete records), over this number"
                  + " in batch, will flush data. The default value is 8MB.");
  protected static final ConfigOption<Integer> SINK_PARALLELISM =
      ConfigOptions.key("sink.parallelism")
          .intType()
          .defaultValue(0)
          .withDescription("this parallelism to sink. The default value is same with task number.");

  protected static final ConfigOption<GroupCommitMode> SINK_GROUP_COMMIT =
      ConfigOptions.key("sink.group-commit")
          .enumType(GroupCommitMode.class)
          .defaultValue(GroupCommitMode.ASYNC_MODE)
          .withDescription(
              "group commit mode include async_mode,sync_mode,off_mode, default async_mode");

  protected static final ConfigOption<String> SINK_COLUMN_SEPARATOR =
      ConfigOptions.key("sink.column-separator").stringType().defaultValue("\\t");

  protected static final ConfigOption<String> SINK_LINE_DELIMITER =
      ConfigOptions.key("sink.line-delimiter").stringType().defaultValue("\\n");

  protected static final ConfigOption<Duration> SINK_FLUSH_INTERVAL =
      ConfigOptions.key("sink.flush-interval")
          .durationType()
          .defaultValue(Duration.ofSeconds(1))
          .withDescription("set how long to flush buffer data to doris, default 1s");

  protected static final ConfigOption<Integer> SINK_THREADS =
      ConfigOptions.key("sink.threads")
          .intType()
          .defaultValue(1)
          .withDescription("the sink threads per task.");

  protected static final ConfigOption<Boolean> AUTO_CREATE_TABLE =
      ConfigOptions.key("auto-create-table")
          .booleanType()
          .defaultValue(false)
          .withDescription("auto create table if not exists");

  protected static final ConfigOption<String> AUTO_CREATE_TABLE_ENGINE =
      ConfigOptions.key("auto-create-table.engine")
          .stringType()
          .defaultValue(null)
          .withDescription("auto create table engine");

  protected static final ConfigOption<String> AUTO_CREATE_TABLE_PARTITION =
      ConfigOptions.key("auto-create-table.partition")
          .stringType()
          .defaultValue(null)
          .withDescription("auto create table partition");

  protected static final ConfigOption<String> AUTO_CREATE_TABLE_BUCKET =
      ConfigOptions.key("auto-create-table.bucket")
          .stringType()
          .defaultValue(null)
          .withDescription("auto create table bucket");

  public static final String AUTO_CREATE_TABLE_PROPERTIES = "auto-create-table.properties.";
  public static final String HEADER_PROPERTIES = "header.properties.";
  public static final String AUTO_CREATE_TABLE_ENGINE_AGGREGATE_FUNCTION =
      "auto-create-table.engine.aggregate-function.";

  protected static final ConfigOption<Duration> AUTO_CREATE_TABLE_PROPERTIES_GROUP_COMMIT_INTERVAL_DURATION =
          ConfigOptions.key(AUTO_CREATE_TABLE_PROPERTIES + "group_commit_interval_ms")
                  .durationType()
                  .defaultValue(Duration.ofMinutes(1))
                  .withDescription("the group commit interval ms properties for auto create table");

  /**
   * group commit mode.
   *
   * @param readableConfig readableConfig
   * @return GroupCommitMode
   */
  public static GroupCommitMode groupCommitMode(ReadableConfig readableConfig) {
    return readableConfig.get(SINK_GROUP_COMMIT);
  }

  /**
   * db.
   *
   * @param readableConfig readableConfig
   * @return db
   */
  public static String db(ReadableConfig readableConfig) {
    final var tableId = readableConfig.get(TABLE_IDENTIFIER);
    final var parts = tableId.split("\\.");
    return parts[0];
  }

  /**
   * table.
   *
   * @param readableConfig readableConfig
   * @return table
   */
  public static String table(ReadableConfig readableConfig) {
    final var tableId = readableConfig.get(TABLE_IDENTIFIER);
    final var parts = tableId.split("\\.");
    return parts[1];
  }
}

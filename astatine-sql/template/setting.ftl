<#macro setting_restart_strategy_policy
    type = 'fixed-delay'
    attempts = '2147483647'
    delay = '10 s'>
SET 'restart-strategy.type' = '${type}';
SET 'restart-strategy.fixed-delay.attempts' = '${attempts}';
SET 'restart-strategy.delay' = '${delay}';
</#macro>

<#macro setting_table
    idle_state_retention_time = '4 min'
    pipeline_operator_chaining='true' >
SET 'table.exec.state.ttl' = '${idle_state_retention_time}';
SET 'pipeline.operator-chaining' = '${pipeline_operator_chaining}';
</#macro>

<#macro setting_checkpointing
    mode = 'EXACTLY_ONCE'
    alignment_timeout = '1 min'
    interval = '2 min'
    timeout = '2 min'
    force_unaligned = 'false'
    max_concurrent = '2'
    min_pause_between = '2 s'
    externalized = 'RETAIN_ON_CANCELLATION'
    tolerable_failed_checkpoints = '5' >
SET 'execution.checkpointing.mode' = '${mode}';
SET 'execution.checkpointing.aligned-checkpoint-timeout' = '${alignment_timeout}';
SET 'execution.checkpointing.interval' = '${interval}';
SET 'execution.checkpointing.timeout' = '${timeout}';
SET 'execution.checkpointing.unaligned.enabled' = '${force_unaligned}';
SET 'execution.checkpointing.max-concurrent-checkpoint' = '${max_concurrent}';
SET 'execution.checkpointing.min-pause' = '${min_pause_between}';
SET 'execution.checkpointing.externalized-checkpoint-retention' = '${externalized}';
SET 'execution.checkpointing.tolerable-failed-checkpoints' = '${tolerable_failed_checkpoints}';
</#macro>
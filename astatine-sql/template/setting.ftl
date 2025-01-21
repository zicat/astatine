<#macro setting_restart_strategy_policy
    type = 'fixed-delay'
    fixed\-delay\.attempts = '2147483647'
    delay = '10 s'>
SET 'restart-strategy.type' = '${type}';
SET 'restart-strategy.fixed-delay.attempts' = '${fixed\-delay\.attempts}';
SET 'restart-strategy.delay' = '${delay}';
</#macro>

<#macro setting_table exec\.state\.ttl = '4 min' >
SET 'table.exec.state.ttl' = '${exec\.state\.ttl}';
</#macro>

<#macro setting_checkpointing
    mode = 'EXACTLY_ONCE'
    aligned\-checkpoint\-timeout = '1 min'
    interval = '2 min'
    timeout = '2 min'
    unaligned\.enabled = 'false'
    max\-concurrent\-checkpoint = '2'
    min\-pause = '2 s'
    externalized\-checkpoint\-retention = 'RETAIN_ON_CANCELLATION'
    tolerable\-failed\-checkpoints = '5' >
SET 'execution.checkpointing.mode' = '${mode}';
SET 'execution.checkpointing.aligned-checkpoint-timeout' = '${aligned\-checkpoint\-timeout}';
SET 'execution.checkpointing.interval' = '${interval}';
SET 'execution.checkpointing.timeout' = '${timeout}';
SET 'execution.checkpointing.unaligned.enabled' = '${unaligned\.enabled}';
SET 'execution.checkpointing.max-concurrent-checkpoint' = '${max\-concurrent\-checkpoint}';
SET 'execution.checkpointing.min-pause' = '${min\-pause}';
SET 'execution.checkpointing.externalized-checkpoint-retention' = '${externalized\-checkpoint\-retention}';
SET 'execution.checkpointing.tolerable-failed-checkpoints' = '${tolerable\-failed\-checkpoints}';
</#macro>
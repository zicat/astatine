<#macro setting_check_point
    mode = 'EXACTLY_ONCE'
    alignment_timeout = '30000'
    interval = '60000'
    timeout = '60000'
    force_unaligned = 'false'
    max_concurrent = '1'
    min_pause_between = '2000'
    externalized = 'RETAIN_ON_CANCELLATION'>
SET 'cp.mode'='${mode}';
SET cp.alignment.timeout=${alignment_timeout};
SET cp.interval=${interval};
SET cp.timeout=${timeout};
SET cp.force.unaligned=${force_unaligned};
SET cp.max.concurrent=${max_concurrent};
SET cp.min.pause.between=${min_pause_between};
SET cp.externalized=${externalized};
</#macro>

<#macro setting_restart_policy type = 'fixed-delay@_,10000'>
SET rs.type=${type};
</#macro>

<#macro setting_table_config idle_state_retention_time = '5' pipeline_operator_chaining='true'
        table_exec_source_idle_auto_create='0s' >
SET tf.idle.state.retention.time=${idle_state_retention_time};
SET tf.pipeline.operator-chaining=${pipeline_operator_chaining};
SET tf.table.exec.source.idle-auto-create=${table_exec_source_idle_auto_create};
</#macro>

<#macro setting
    cp_mode = 'EXACTLY_ONCE'
    cp_alignment_timeout = '30000'
    cp_interval = '60000'
    cp_timeout = '60000'
    cp_force_unaligned = 'false'
    cp_max_concurrent = '1'
    cp_min_pause_between = '2000'
    cp_externalized = 'RETAIN_ON_CANCELLATION'
    rs_type = 'fixed-delay@_,10000'
    tf_idle_state_retention_time = '5'
    tf_pipeline_operator_chaining='true'
    tf_table_exec_source_idle_auto_create='0s' >
    <@setting_check_point
        mode = cp_mode
        alignment_timeout = cp_alignment_timeout
        interval = cp_interval
        timeout = cp_timeout
        force_unaligned = cp_force_unaligned
        max_concurrent = cp_max_concurrent
        min_pause_between = cp_min_pause_between
        externalized = cp_externalized />
    <@setting_restart_policy type = rs_type />
    <@setting_table_config
        idle_state_retention_time = tf_idle_state_retention_time
        pipeline_operator_chaining = tf_pipeline_operator_chaining
        table_exec_source_idle_auto_create = tf_table_exec_source_idle_auto_create />
</#macro>
from .types import (
    SettingUInt64, SettingBool, SettingFloat, SettingString, SettingMaxThreads
)

SettingInt64 = SettingUInt64

# Seconds and milliseconds should be set as ints.
SettingSeconds = SettingMilliseconds = SettingUInt64

# Server cares about possible choices validation.
# See https://github.com/yandex/ClickHouse/blob/master/dbms/src/
# Interpreters/SettingsCommon.h for all choices.
SettingLoadBalancing = SettingTotalsMode = SettingCompressionMethod = \
    SettingDistributedProductMode = SettingGlobalSubqueriesMethod = \
    SettingString

settings = {
    'min_compress_block_size': SettingUInt64,
    'max_compress_block_size': SettingUInt64,
    'max_block_size': SettingUInt64,
    'max_insert_block_size': SettingUInt64,
    'min_insert_block_size_rows': SettingUInt64,
    'min_insert_block_size_bytes': SettingUInt64,
    'max_threads': SettingMaxThreads,
    'max_read_buffer_size': SettingUInt64,
    'max_distributed_connections': SettingUInt64,
    'max_query_size': SettingUInt64,
    'interactive_delay': SettingUInt64,
    'connect_timeout': SettingSeconds,
    'connect_timeout_with_failover_ms': SettingMilliseconds,
    'receive_timeout': SettingSeconds,
    'send_timeout': SettingSeconds,
    'queue_max_wait_ms': SettingMilliseconds,
    'poll_interval': SettingUInt64,
    'distributed_connections_pool_size': SettingUInt64,
    'connections_with_failover_max_tries': SettingUInt64,
    'extremes': SettingBool,
    'use_uncompressed_cache': SettingBool,
    'replace_running_query': SettingBool,
    'background_pool_size': SettingUInt64,
    'distributed_directory_monitor_sleep_time_ms': SettingMilliseconds,
    'distributed_directory_monitor_batch_inserts': SettingBool,
    'optimize_move_to_prewhere': SettingBool,
    'replication_alter_partitions_sync': SettingUInt64,
    'replication_alter_columns_timeout': SettingUInt64,
    'load_balancing': SettingLoadBalancing,
    'totals_mode': SettingTotalsMode,
    'totals_auto_threshold': SettingFloat,
    'compile': SettingBool,
    'min_count_to_compile': SettingUInt64,
    'group_by_two_level_threshold': SettingUInt64,
    'group_by_two_level_threshold_bytes': SettingUInt64,
    'distributed_aggregation_memory_efficient': SettingBool,
    'aggregation_memory_efficient_merge_threads': SettingUInt64,
    'max_parallel_replicas': SettingUInt64,
    'parallel_replicas_count': SettingUInt64,
    'parallel_replica_offset': SettingUInt64,
    'skip_unavailable_shards': SettingBool,
    'distributed_group_by_no_merge': SettingBool,
    'merge_tree_min_rows_for_concurrent_read': SettingUInt64,
    'merge_tree_min_rows_for_seek': SettingUInt64,
    'merge_tree_coarse_index_granularity': SettingUInt64,
    'merge_tree_max_rows_to_use_cache': SettingUInt64,
    'merge_tree_uniform_read_distribution': SettingBool,
    'optimize_min_equality_disjunction_chain_length': SettingUInt64,
    'min_bytes_to_use_direct_io': SettingUInt64,
    'force_index_by_date': SettingBool,
    'force_primary_key': SettingBool,
    'strict_insert_defaults': SettingBool,
    'mark_cache_min_lifetime': SettingUInt64,
    'max_streams_to_max_threads_ratio': SettingFloat,
    'network_compression_method': SettingCompressionMethod,
    'priority': SettingUInt64,
    'log_queries': SettingBool,
    'log_queries_cut_to_length': SettingUInt64,
    'distributed_product_mode': SettingDistributedProductMode,
    'global_subqueries_method': SettingGlobalSubqueriesMethod,
    'max_concurrent_queries_for_user': SettingUInt64,
    'insert_quorum': SettingUInt64,
    'insert_quorum_timeout': SettingMilliseconds,
    'select_sequential_consistency': SettingUInt64,
    'table_function_remote_max_addresses': SettingUInt64,
    'max_distributed_processing_threads': SettingUInt64,
    'read_backoff_min_latency_ms': SettingMilliseconds,
    'read_backoff_max_throughput': SettingUInt64,
    'read_backoff_min_interval_between_events_ms': SettingMilliseconds,
    'read_backoff_min_events': SettingUInt64,
    'memory_tracker_fault_probability': SettingFloat,
    'enable_http_compression': SettingBool,
    'http_zlib_compression_level': SettingInt64,
    'http_native_compression_disable_checksumming_on_decompress': SettingBool,
    'resharding_barrier_timeout': SettingUInt64,
    'count_distinct_implementation': SettingString,
    'output_format_write_statistics': SettingBool,
    'add_http_cors_header': SettingBool,
    'input_format_skip_unknown_fields': SettingBool,
    'input_format_values_interpret_expressions': SettingBool,
    'output_format_json_quote_64bit_integers': SettingBool,
    'output_format_json_quote_denormals': SettingBool,
    'output_format_pretty_max_rows': SettingUInt64,
    'use_client_time_zone': SettingBool,
    'send_progress_in_http_headers': SettingBool,
    'http_headers_progress_interval_ms': SettingUInt64,
    'fsync_metadata': SettingBool,
    'input_format_allow_errors_num': SettingUInt64,
    'input_format_allow_errors_ratio': SettingFloat,
    'join_use_nulls': SettingBool,
    'preferred_block_size_bytes': SettingUInt64,
    'max_replica_delay_for_distributed_queries': SettingUInt64,
    'fallback_to_stale_replicas_for_distributed_queries': SettingBool,
    'distributed_ddl_allow_replicated_alter': SettingBool,
}


# See https://github.com/yandex/ClickHouse/blob/master/dbms/src/
# Interpreters/Limits.h for all choices.
limits = {
    'max_rows_to_read': SettingUInt64,
    'max_bytes_to_read': SettingUInt64,
    'read_overflow_mode': SettingString,

    'max_rows_to_group_by': SettingUInt64,
    'group_by_overflow_mode': SettingString,
    'max_bytes_before_external_group_by': SettingUInt64,

    'max_rows_to_sort': SettingUInt64,
    'max_bytes_to_sort': SettingUInt64,
    'sort_overflow_mode': SettingString,
    'max_bytes_before_external_sort': SettingUInt64,

    'max_result_rows': SettingUInt64,
    'max_result_bytes': SettingUInt64,
    'result_overflow_mode': SettingString,

    'max_execution_time': SettingSeconds,
    'timeout_overflow_mode': SettingString,

    'min_execution_speed': SettingUInt64,

    'timeout_before_checking_execution_speed': SettingSeconds,

    'max_columns_to_read': SettingUInt64,
    'max_temporary_columns': SettingUInt64,
    'max_temporary_non_const_columns': SettingUInt64,

    'max_subquery_depth': SettingUInt64,
    'max_pipeline_depth': SettingUInt64,
    'max_ast_depth': SettingUInt64,
    'max_ast_elements': SettingUInt64,

    'readonly': SettingUInt64,

    'max_rows_in_set': SettingUInt64,
    'max_bytes_in_set': SettingUInt64,
    'set_overflow_mode': SettingString,

    'max_rows_in_join': SettingUInt64,
    'max_bytes_in_join': SettingUInt64,
    'join_overflow_mode': SettingString,

    'max_rows_to_transfer': SettingUInt64,
    'max_bytes_to_transfer': SettingUInt64,
    'transfer_overflow_mode': SettingString,

    'max_rows_in_distinct': SettingUInt64,
    'max_bytes_in_distinct': SettingUInt64,
    'distinct_overflow_mode': SettingString,

    'max_memory_usage': SettingUInt64,

    'max_memory_usage_for_user': SettingUInt64,

    'max_memory_usage_for_all_queries': SettingUInt64,

    'max_network_bandwidth': SettingUInt64,
    'max_network_bytes': SettingUInt64
}

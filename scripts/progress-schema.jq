def number_or_null: (type == "number" or type == "null");
def integer_or_null: (type == "number" and floor == .) or type == "null";
def known_counter:
  .key == "phase" or (.value | number_or_null);
def known_resource:
  .key == "loadAverage" or .key == "processes" or (.value | number_or_null);
def valid_process:
  type == "object"
  and ((keys_unsorted - ["pid","ppid","state","elapsed","cpuPercent","memoryPercent","rssKb","command"]) | length == 0)
  and (.pid | integer_or_null)
  and (.ppid | integer_or_null)
  and (.state | type == "string")
  and (.elapsed | type == "string")
  and (.cpuPercent | number_or_null)
  and (.memoryPercent | number_or_null)
  and (.rssKb | integer_or_null)
  and (.command | type == "string" and length <= 128);

type == "object"
and ((keys_unsorted - ["runId","runAttempt","timestamp","state","phase","counters","resources","exitCode"]) | length == 0)
and (.runId | type == "string" and test("^[A-Za-z0-9_.-]+$"))
and (.runAttempt | type == "string" and test("^[A-Za-z0-9_.-]+$"))
and (.timestamp | type == "string" and test("^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}Z$"))
and (.state | type == "string" and test("^(running|success|failure|cancelled)$"))
and (.phase | type == "string" and length <= 64)
and (.counters | type == "object"
  and ((keys_unsorted - ["phase","elapsed_ms","throughput_records_per_sec","compressed_bytes","input_bytes","input_lines","raw_parsed","accepted","invalid_lines","skipped","skipped_invalid_barcode","skipped_missing_name","skipped_missing_nutrition","skipped_validation","sort_runs","deduped_docs","indexed_docs","phase_bytes","phase_files"]) | length == 0)
  and all(to_entries[]?; known_counter))
and (.resources | type == "object"
  and ((keys_unsorted - ["elapsedSeconds","diskFreeKb","memoryTotalKb","memoryUsedKb","memoryAvailableKb","cpuCount","cpuPercent","loadAverage","processes"]) | length == 0)
  and all(to_entries[]?; known_resource)
  and (if has("processes") then (.processes | type == "array" and all(.[]; valid_process)) else true end))
and (if has("exitCode") then (.exitCode | type == "number" and floor == .) else true end)

$LOAD_PATH.unshift(File.dirname(__FILE__))
$LOAD_PATH.unshift(File.join(File.dirname(__FILE__), "..", "lib"))

require_relative 'feather_weight_benchmark'
require_relative 'light_weight_benchmark'
require_relative 'middle_weight_benchmark'
require_relative 'heavy_weight_benchmark'

#def MMABench
  feather_flat_bson, feather_nested_bson, feather_full_bson = featherweight_benchmark!
  #light_run_command, light_small_insert, light_large_insert = lightweight_benchmark!
  #middle_find, middle_small_insert, middle_large_insert, middle_gridfs_upload, middle_gridfs_download = middleweight_benchmark!
  #heavy_ldjson_import, heavy_ldjson_export, heavy_gridfs_upload, heavy_gridfs_download = heavyweight_benchmark!

  # TODO: get above methods to return resulting number, or whatever relevant data

  p feather_flat_bson#.first.real
  p feather_nested_bson#.first.real
  p feather_full_bson#.first.real

# Get median, primary results
p BenchmarkHelper.median(feather_flat_bson)
p BenchmarkHelper.median(feather_nested_bson)
p BenchmarkHelper.median(feather_full_bson)
# 10th, 25th, 50th, 75th, 90th, 95th, 98th and 99th percentiles
p BenchmarkHelper.percentile_values(feather_flat_bson)
p BenchmarkHelper.percentile_values(feather_nested_bson)
p BenchmarkHelper.percentile_values(feather_full_bson)

#end
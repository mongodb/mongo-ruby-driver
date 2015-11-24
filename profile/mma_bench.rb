$LOAD_PATH.unshift(File.dirname(__FILE__))
$LOAD_PATH.unshift(File.join(File.dirname(__FILE__), "..", "lib"))

require_relative 'feather_weight_benchmark'
require_relative 'light_weight_benchmark'
require_relative 'middle_weight_benchmark'
require_relative 'heavy_weight_benchmark'

def mma_bench

  # Run benchmarks
  featherweight = featherweight_benchmark(3)
  lightweight = lightweight_benchmark(3)
  middleweight = middleweight_benchmark(3)
  heavyweight = heavyweight_benchmark(3)

  # FeatherBench
  feather_bench_results = featherweight.map do |test_results|
    BenchmarkHelper.MB_per_second(test_results[0], test_results[1])
  end
  p "FeatherBench (MB/Second)"
  p BenchmarkHelper.mmabench_composite_score(feather_bench_results)

  # LightBench
  lightweight_bench_results = lightweight.map do |test_results|
    BenchmarkHelper.MB_per_second(test_results[0], test_results[1])
  end
  p "LightBench (MB/Second)"
  p BenchmarkHelper.mmabench_composite_score(lightweight_bench_results)

  # MiddleBench
  middleweight_bench_results = middleweight.map do |test_results|
    BenchmarkHelper.MB_per_second(test_results[0], test_results[1])
  end
  p "MiddleBench (MB/Second)"
  p BenchmarkHelper.mmabench_composite_score(middleweight_bench_results)

  # HeavyBench
  heavyweight_bench_results = heavyweight.map do |test_results|
    BenchmarkHelper.MB_per_second(test_results[0], test_results[1])
  end
  p "HeavyBench (MB/Second)"
  p BenchmarkHelper.mmabench_composite_score(heavyweight_bench_results)

  # ReadBench - Average of "Run command", "Find one by ID", "Find many and empty cursor",
  #            "GridFS download", "LDJSON multi-file export", and "GridFS multi-file download" microbenchmarks
  read_bench = [lightweight_bench_results[0], lightweight_bench_results[1], middleweight_bench_results[0],
                middleweight_bench_results[4], heavyweight_bench_results[1], heavyweight_bench_results[3]]
  read_bench_result = BenchmarkHelper.mmabench_composite_score(read_bench)
  p "ReadBench (MB/Second)"
  p read_bench_result

  # WriteBench - Average of "Small doc insert one", "Large doc insert one", "Small doc bulk insert",
  #              "Large doc bulk insert", "GridFS upload", "LDJSON multi-file import", and "GridFS multi-file upload"
  #              micro-benchmarks
  write_bench = [lightweight_bench_results[2], lightweight_bench_results[3], middleweight_bench_results[1],
                 middleweight_bench_results[2], middleweight_bench_results[3], heavyweight_bench_results[0],
                 heavyweight_bench_results[2]]
  write_bench_result = BenchmarkHelper.mmabench_composite_score(write_bench)
  p "WriteBench (MB/Second)"
  p write_bench_result

  # MMABench - Average of ReadBench and WriteBench
  p "MMABench (MB/Second)"
  p BenchmarkHelper.mmabench_composite_score([read_bench_result, write_bench_result])

end

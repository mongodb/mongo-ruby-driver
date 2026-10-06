# frozen_string_literal: true

$LOAD_PATH.unshift File.expand_path('../../../lib', __dir__)

namespace :otel_attributes do
  desc 'Measures the SDK cost of each OpenTelemetry span attribute shape'
  task :span do
    require_relative '../span_cost'

    puts Mongo::OtelAttributes::SpanCost.run!
  end

  desc 'Measures the driver cost of building each command span attribute'
  task :construction do
    require_relative '../construction_cost'

    puts Mongo::OtelAttributes::ConstructionCost.run!
  end

  desc 'Runs both attribute-cost harnesses'
  task all: %i[ span construction ]

  desc 'Measures per-attribute cost relative to the untraced operation on this host'
  task :evergreen do
    require 'json'
    require 'tmpdir'

    baseline_tasks = 'small doc insertone,find one by id'
    baseline_file = File.join(Dir.mktmpdir('otel-attribute-baseline'), 'baseline.json')

    # The relative figures are a percentage of the untraced operation, so that
    # operation has to be measured on the same host as the spans.
    puts '===== Untraced baseline (this host) ====='
    ok = system(
      { 'DRIVER_BENCH_TASKS' => baseline_tasks, 'PERFORMANCE_RESULTS_FILE' => baseline_file },
      'bundle', 'exec', 'rake', 'driver_bench'
    )
    raise 'the untraced baseline run failed' unless ok

    baseline = JSON.parse(File.read(baseline_file))
    baseline_cpu = lambda do |data, test_name|
      entry = data.find { |item| item['info']['test_name'] == test_name }
      raise "no untraced baseline for #{test_name.inspect}" unless entry

      metric = entry['metrics'].find { |item| item['name'] == 'cpu_ex_gc_us_per_op' }
      raise "no cpu_ex_gc_us_per_op for #{test_name.inspect}" unless metric

      metric['value']
    end
    ENV['REFERENCE_FIND_CPU_US'] = baseline_cpu.call(baseline, 'Find one by ID').to_s
    ENV['REFERENCE_INSERT_CPU_US'] = baseline_cpu.call(baseline, 'Small doc insertOne').to_s

    require_relative '../span_cost'
    require_relative '../construction_cost'

    puts Mongo::OtelAttributes::SpanCost.run!
    puts
    puts Mongo::OtelAttributes::ConstructionCost.run!
  end
end

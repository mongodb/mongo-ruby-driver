# frozen_string_literal: true

require 'fileutils'
require 'json'
require 'tmpdir'

require_relative 'configuration'
require_relative 'percentiles'
require_relative 'suite'

module Mongo
  module DriverBench
    # Runs the DriverBench micro-benchmarks under several driver
    # configurations and reports what each configuration costs relative to the
    # baseline.
    #
    # OpenTelemetry can only be installed into a process once, and the
    # "api-only" configuration requires that the SDK was never loaded, so each
    # configuration is measured in its own subprocess.
    #
    # Configurations are run interleaved -- the whole set, then the whole set
    # again -- rather than one configuration to completion and then the next.
    # The quantity being measured is a difference between two configurations,
    # and a machine that slows down halfway through a run would otherwise put
    # all of one configuration's samples in the fast half and all of another's
    # in the slow half, which shows up as overhead that is not there.
    #
    # The order is also rotated by one position each repetition, so that no
    # configuration always runs in the same slot. With a fixed order, a
    # slot-dependent effect showed up as a bias of its own: sdk-parent-1pct
    # measured cheaper than sdk-never in every run, though it does strictly
    # more work. With as many repetitions as configurations, each one runs
    # in every slot exactly once.
    #
    # Parameterised by the environment:
    #
    #   CONFIGURATIONS  comma-separated configuration names (default: all)
    #   REPS            repetitions of the whole set (default 1)
    #   MONGODB_URI     server to benchmark
    #   DRIVER_BENCH_TASKS
    #   DRIVER_BENCH_EXCLUDE_TASKS
    #                   narrow the micro-benchmarks, as for Suite. When
    #                   neither is set, BSON micro-benchmarks are skipped:
    #                   they never talk to a server, so they create no spans
    #                   and can only add noise to the comparison.
    #   PERFORMANCE_RESULTS_FILE
    #                   where the combined results are written
    #   DRIVER_BENCH_ENFORCE_TARGETS
    #                   fail when a configuration exceeds its target
    #   DRIVER_BENCH_MAX_ITERATIONS, DRIVER_BENCH_MIN_TIME
    #                   passed through to every run, see Base
    #
    # @api private
    class Comparison
      DEFAULT_EXCLUDED_TASKS = 'BSON'
      DEFAULT_RESULTS_FILE = 'perf-comparison.json'

      # Metrics carried from every child run into the comparison. Each is
      # reduced to its median across repetitions.
      CARRIED_METRICS = %w[
        score wall_us_per_op cpu_us_per_op cpu_ex_gc_us_per_op gc_us_per_op allocs_per_op
        time-90% time-99% spans_per_op
      ].freeze

      TABLE_FORMAT = '%-22<task>s %-16<config>s %9<loss>s %8<target>s %-4<verdict>s ' \
                     '%15<spread>s %11<cpu>s %9<cpu_pct>s %9<gc>s %9<allocs>s %6<spans>s'

      def self.run!
        new.run
      end

      def initialize(env = ENV)
        @env = env
        @reps = Integer(env['REPS'] || 1)
        @configurations = requested_configurations
        @samples = Hash.new { |hash, key| hash[key] = Hash.new { |h, k| h[k] = [] } }
      end

      # Runs every configuration, @reps times, and reports the comparison.
      #
      # @return [ String ] the summary table.
      def run
        raise 'the baseline configuration is required to compare against' unless
          @configurations.any?(&:baseline?)

        # Checked here as well as in every child, to fail before the first
        # of many long runs rather than inside it.
        Configuration.check_jit!

        Dir.mktmpdir('driver-bench') do |dir|
          1.upto(@reps) do |rep|
            @configurations.rotate(rep - 1).each { |configuration| measure(configuration, rep, dir) }
          end
        end

        save_perf_data(compile_perf_data)
        summary = summarize
        enforce_targets!(summary)
        summary
      end

      private

      def requested_configurations
        names = (@env['CONFIGURATIONS'] || '').split(',').map(&:strip).reject(&:empty?)
        return Configuration::ALL if names.empty?

        names.map { |name| Configuration[name] }
      end

      def baseline
        @baseline ||= @configurations.find(&:baseline?)
      end

      # Runs one configuration once, in its own process, and records the
      # metrics of every micro-benchmark it reported.
      def measure(configuration, rep, dir)
        results_file = File.join(dir, "#{configuration.name}-#{rep}.json")

        puts format("\n----- rep %d/%d: %s -----", rep, @reps, configuration.name)
        unless system(child_env(configuration, rep, results_file), 'bundle', 'exec', 'rake', 'driver_bench:run')
          raise "configuration #{configuration.name} failed in rep #{rep}"
        end

        JSON.parse(File.read(results_file)).each do |entry|
          samples = @samples[[ configuration.task_name(entry['info']['test_name']), configuration.name ]]
          entry['metrics'].each do |metric|
            samples[metric['name']] << metric['value'] if CARRIED_METRICS.include?(metric['name'])
          end
        end
      end

      def child_env(configuration, rep, results_file)
        {
          Configuration::ENV_VAR => configuration.name,
          'PERFORMANCE_RESULTS_FILE' => results_file,
          'DRIVER_BENCH_EXCLUDE_TASKS' => excluded_tasks,
          # Span counts do not vary between repetitions; count them once.
          'DRIVER_BENCH_COUNT_SPANS' => (rep == 1).to_s
        }
      end

      def excluded_tasks
        return @env['DRIVER_BENCH_EXCLUDE_TASKS'].to_s if
          @env['DRIVER_BENCH_EXCLUDE_TASKS'] || @env['DRIVER_BENCH_TASKS']

        DEFAULT_EXCLUDED_TASKS
      end

      # The median of one metric across repetitions, for the same reason the
      # spec takes the median across iterations.
      def median(task, configuration, metric)
        values = @samples[[ task, configuration.name ]][metric]
        return nil if values.empty?

        Percentiles.new(values)[50]
      end

      def spread(task, configuration)
        values = @samples[[ task, configuration.name ]]['score']
        return nil if values.empty?

        values.minmax
      end

      # Scores are throughput, so enabling a feature shows up as a loss.
      #
      # @return [ Float | nil ] the percentage of throughput given up,
      #   relative to the baseline.
      def loss_for(task, configuration)
        reference = median(task, baseline, 'score')
        score = median(task, configuration, 'score')
        return nil if reference.nil? || score.nil? || reference.zero?

        (reference - score) / reference * 100.0
      end

      # CPU time and allocations grow with cost, so the overhead is the
      # difference over the baseline.
      def added(task, configuration, metric)
        reference = median(task, baseline, metric)
        value = median(task, configuration, metric)
        return nil if reference.nil? || value.nil?

        value - reference
      end

      def cpu_overhead_pct(task, configuration, metric = 'cpu_us_per_op')
        reference = median(task, baseline, metric)
        delta = added(task, configuration, metric)
        return nil if delta.nil? || reference.zero?

        delta / reference * 100.0
      end

      # Spans are only countable under a configuration that records every
      # span, but the count is a property of the task, so it is reported
      # for the task as a whole.
      def spans_per_op(task)
        @configurations.each do |configuration|
          value = median(task, configuration, 'spans_per_op')
          return value unless value.nil?
        end
        nil
      end

      # Targets are checked against the CPU overhead (excluding GC where it
      # is reported), not the throughput loss. Throughput includes waiting on
      # the server and moved by 2-3 points between runs, enough to flip a
      # configuration across its target; it also ranked configurations in an
      # order their work rules out (sdk-never above api-only).
      def target_met(task, configuration)
        overhead = cpu_overhead_pct(task, configuration, cpu_metric(task))
        return nil if overhead.nil? || configuration.target_pct.nil?

        overhead <= configuration.target_pct
      end

      def tasks
        @tasks ||= @samples.keys.map(&:first).uniq
      end

      def compile_perf_data
        tasks.flat_map do |task|
          @configurations.filter_map do |configuration|
            next if median(task, configuration, 'score').nil?

            {
              'info' => {
                'test_name' => configuration.perf_test_name(task),
                'args' => {}
              },
              'metrics' => metrics_for(task, configuration)
            }
          end
        end
      end

      def metrics_for(task, configuration)
        metrics = CARRIED_METRICS.filter_map do |name|
          value = median(task, configuration, name)
          { 'name' => name, 'value' => value } unless value.nil?
        end
        min, max = spread(task, configuration)
        metrics << { 'name' => 'score_min', 'value' => min } << { 'name' => 'score_max', 'value' => max }
        return metrics if configuration.baseline?

        # The overheads are recorded as metrics of their own, rather than
        # left to be derived by comparing two time series later, so that
        # they can be watched for regressions directly and so that
        # host-to-host variation cancels out of them.
        {
          'overhead_pct' => loss_for(task, configuration),
          'cpu_overhead_pct' => cpu_overhead_pct(task, configuration),
          'cpu_us_added_per_op' => added(task, configuration, 'cpu_us_per_op'),
          'cpu_ex_gc_overhead_pct' => cpu_overhead_pct(task, configuration, 'cpu_ex_gc_us_per_op'),
          'cpu_ex_gc_us_added_per_op' => added(task, configuration, 'cpu_ex_gc_us_per_op'),
          'gc_us_added_per_op' => added(task, configuration, 'gc_us_per_op'),
          'allocs_added_per_op' => added(task, configuration, 'allocs_per_op'),
          'target_pct' => configuration.target_pct
        }.each do |name, value|
          metrics << { 'name' => name, 'value' => value } unless value.nil?
        end
        metrics
      end

      def save_perf_data(data, file_name: @env['PERFORMANCE_RESULTS_FILE'] || DEFAULT_RESULTS_FILE)
        File.write(file_name, data.to_json)
      end

      def summarize
        lines = [ format("\n===== Configuration comparison (%d rep%s, median) =====",
                         @reps, (@reps == 1) ? '' : 's') ]
        lines << 'loss: throughput given up vs off; target: max cpu %; spread: min-max MB/s across reps; ' \
                 'cpu: CPU us added per op, excluding GC; gc: GC us added per op; ' \
                 'allocs: objects added per op'
        lines << ''
        lines << format(TABLE_FORMAT,
                        task: 'micro-benchmark', config: 'configuration', loss: 'loss %',
                        target: 'target', verdict: 'ok?', spread: 'spread MB/s',
                        cpu: 'cpu us/op', cpu_pct: 'cpu %', gc: 'gc us/op', allocs: 'allocs', spans: 'spans')
        tasks.each do |task|
          @configurations.reject(&:baseline?).each { |configuration| lines << row(task, configuration) }
        end

        lines.join("\n")
      end

      def row(task, configuration)
        min, max = spread(task, configuration)

        format(TABLE_FORMAT,
               task: task, config: configuration.name,
               loss: signed(loss_for(task, configuration)),
               target: configuration.target_pct ? format('%g', configuration.target_pct) : '-',
               verdict: verdict(target_met(task, configuration)),
               spread: min ? format('%.4g-%.4g', min, max) : 'n/a',
               cpu: signed(added(task, configuration, cpu_metric(task))),
               cpu_pct: signed(cpu_overhead_pct(task, configuration, cpu_metric(task))),
               gc: signed(added(task, configuration, 'gc_us_per_op')),
               allocs: signed(added(task, configuration, 'allocs_per_op'), '%+.0f'),
               spans: (spans = spans_per_op(task)) ? format('%.2g', spans) : 'n/a')
      end

      # CPU time excluding GC where the runtime reports GC time, total CPU
      # time otherwise.
      def cpu_metric(task)
        median(task, baseline, 'cpu_ex_gc_us_per_op') ? 'cpu_ex_gc_us_per_op' : 'cpu_us_per_op'
      end

      def signed(value, pattern = '%+.2f')
        value.nil? ? 'n/a' : format(pattern, value)
      end

      def verdict(within)
        return '-' if within.nil?

        within ? 'yes' : 'NO'
      end

      # Fails the run when a configuration misses its target, if asked to
      # (DRIVER_BENCH_ENFORCE_TARGETS). The results are saved and printed
      # first, so a failing run still shows why.
      def enforce_targets!(summary)
        return unless %w[1 true yes].include?(@env['DRIVER_BENCH_ENFORCE_TARGETS'].to_s.downcase)

        misses = tasks.product(@configurations).select { |task, configuration| target_met(task, configuration) == false }
        return if misses.empty?

        puts summary
        raise "over target: #{misses.map { |task, configuration| "#{task} / #{configuration.name}" }.join(', ')}"
      end
    end
  end
end

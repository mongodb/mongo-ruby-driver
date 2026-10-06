# frozen_string_literal: true

require 'fileutils'
require 'json'
require 'opentelemetry-sdk'

require_relative 'profiles'

module Mongo
  module OtelAttributes
    # Measures what each OpenTelemetry span attribute costs at the SDK
    # boundary: the cost of carrying an attribute through start_span, and of
    # setting one after creation.
    #
    # Values are fixed literals (see Profiles::SAMPLE), so only the SDK's
    # handling of the attribute is measured. The driver's cost of building the
    # value is measured by ConstructionCost.
    #
    # Every profile runs in one process and the order rotates each repetition,
    # the control the DriverBench comparison uses: the quantity of interest is
    # a difference between profiles, and a machine that slows down partway
    # through a run would otherwise charge the drift to whichever profile ran
    # in the slow half.
    #
    # The sweep runs twice, under a recording sampler and under a dropping one,
    # because a deferred attribute costs nothing when the span is not
    # recording: set_attribute discards.
    #
    # Parameterised by the environment:
    #   ITERATIONS    spans per sample (default 50000)
    #   REPS          repetitions of the whole profile set (default 7)
    #   RESULTS_FILE  where the JSONL rows are written
    #
    # @api private
    class SpanCost
      SPAN_NAME = 'find perftest.corpus'
      DEFAULT_ITERATIONS = 50_000
      DEFAULT_REPS = 7
      DEFAULT_RESULTS_FILE = File.expand_path('../../tmp/otel-attribute-span-cost.jsonl', __dir__)
      FORMAT = '%-32<profile>s %9<cpu>s %9<dcpu>s %10<allocs>s %9<dallocs>s %9<gc>s %9<wall>s'

      # One timed sample: the cost of one profile over one repetition, per span.
      Sample = Struct.new(:cpu_us_per_span, :gc_us_per_span, :wall_us_per_span, :allocs_per_span)

      def self.run!
        new.run
      end

      def initialize(env = ENV)
        @iterations = Integer(env['ITERATIONS'] || DEFAULT_ITERATIONS)
        @reps = Integer(env['REPS'] || DEFAULT_REPS)
        @results_file = env['RESULTS_FILE'] || DEFAULT_RESULTS_FILE
        @samples = Hash.new { |hash, key| hash[key] = [] }
      end

      # Runs the sweep and returns the summary tables.
      #
      # @return [ String ] the summary.
      def run
        FileUtils.mkdir_p(File.dirname(@results_file))
        File.write(@results_file, '')
        samplers.each { |label, tracer| sweep(label, tracer) }
        summarize
      end

      private

      def samplers
        {
          'recording' => tracer(::OpenTelemetry::SDK::Trace::Samplers::ALWAYS_ON),
          'non-recording' => tracer(::OpenTelemetry::SDK::Trace::Samplers::ALWAYS_OFF)
        }
      end

      def tracer(sampler)
        ::OpenTelemetry::SDK::Trace::TracerProvider.new(sampler: sampler)
                                                   .tracer('otel-attribute-cost', '1.0')
      end

      def sweep(label, tracer)
        profiles = OtelAttributes.profiles
        profiles.each { |profile| measure(tracer, profile, [ @iterations / 10, 1_000 ].max) }
        1.upto(@reps) do |rep|
          profiles.rotate(rep - 1).each do |profile|
            record(label, profile, rep, measure(tracer, profile))
          end
        end
      end

      # Times one profile over +iterations+ spans.
      #
      # @return [ Sample ] the per-span costs.
      def measure(tracer, profile, iterations = @iterations)
        creation = profile.creation_attributes
        deferred = profile.deferred_pairs
        GC.start
        allocs_before = GC.stat(:total_allocated_objects)
        gc_before = GC.stat(:time)
        cpu_before = Process.clock_gettime(Process::CLOCK_PROCESS_CPUTIME_ID)
        wall_before = Process.clock_gettime(Process::CLOCK_MONOTONIC)
        iterations.times { run_once(tracer, profile.bare, creation, deferred) }
        cpu = (Process.clock_gettime(Process::CLOCK_PROCESS_CPUTIME_ID) - cpu_before) / iterations * 1e6
        wall = (Process.clock_gettime(Process::CLOCK_MONOTONIC) - wall_before) / iterations * 1e6
        gc = (GC.stat(:time) - gc_before) / 1000.0 / iterations * 1e6
        Sample.new(cpu - gc, gc, wall, (GC.stat(:total_allocated_objects) - allocs_before).to_f / iterations)
      end

      def run_once(tracer, bare, creation, deferred)
        span = if bare
                 tracer.start_span(SPAN_NAME, kind: :client)
               else
                 tracer.start_span(SPAN_NAME, attributes: creation, kind: :client)
               end
        deferred.each { |key, value| span.set_attribute(key, value) } if span.recording?
        span.finish
      end

      def record(label, profile, rep, sample)
        @samples[[ label, profile.name ]] << sample
        File.open(@results_file, 'a') do |file|
          file.puts(JSON.generate(sampler: label, profile: profile.name, rep: rep,
                                  cpu_us_per_span: sample.cpu_us_per_span,
                                  gc_us_per_span: sample.gc_us_per_span,
                                  wall_us_per_span: sample.wall_us_per_span,
                                  allocs_per_span: sample.allocs_per_span))
        end
      end

      def median(label, profile_name, field)
        values = @samples[[ label, profile_name ]].map(&field).sort
        return nil if values.empty?

        mid = values.length / 2
        values.length.odd? ? values[mid] : (values[mid - 1] + values[mid]) / 2.0
      end

      def summarize
        labels = @samples.keys.map(&:first).uniq
        labels.flat_map { |label| [ table(label), '' ] }.join("\n")
      end

      def table(label)
        reference_cpu = median(label, 'none', :cpu_us_per_span)
        reference_allocs = median(label, 'none', :allocs_per_span)
        lines = [
          format('===== Span attribute cost at the SDK boundary: %s (median of %d reps, %d spans/sample) =====',
                 label, @reps, @iterations),
          'cpu: CPU us per span excluding GC; allocs: objects per span; ' \
          'd-cpu/d-allocs: added over the zero-attribute span'
        ]
        lines << format(FORMAT, profile: 'profile', cpu: 'cpu us', dcpu: 'd-cpu', allocs: 'allocs',
                                dallocs: 'd-allocs', gc: 'gc us', wall: 'wall us')
        OtelAttributes.profiles.each do |profile|
          lines << row(label, profile.name, reference_cpu, reference_allocs)
        end
        lines.join("\n")
      end

      def row(label, profile_name, reference_cpu, reference_allocs)
        cpu = median(label, profile_name, :cpu_us_per_span)
        allocs = median(label, profile_name, :allocs_per_span)
        format(FORMAT,
               profile: profile_name,
               cpu: number(cpu), dcpu: delta(cpu, reference_cpu),
               allocs: number(allocs), dallocs: delta(allocs, reference_allocs),
               gc: number(median(label, profile_name, :gc_us_per_span)),
               wall: number(median(label, profile_name, :wall_us_per_span)))
      end

      def number(value)
        value.nil? ? 'n/a' : format('%.3f', value)
      end

      def delta(value, reference)
        return 'n/a' if value.nil? || reference.nil?

        format('%+.3f', value - reference)
      end
    end
  end
end

puts Mongo::OtelAttributes::SpanCost.run! if $PROGRAM_NAME == __FILE__

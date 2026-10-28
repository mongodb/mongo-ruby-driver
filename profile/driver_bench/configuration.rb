# frozen_string_literal: true

module Mongo
  module DriverBench
    # One driver configuration under which the DriverBench tasks can be run.
    #
    # A benchmark result is identified by the pair (task, configuration), so
    # every configuration becomes its own time series and can be watched for
    # regressions independently. Comparing a configuration against the
    # baseline is what catches a performance regression in an opt-in feature
    # such as OpenTelemetry: no benchmark that only ever runs the default
    # configuration will execute that code at all.
    #
    # The configurations are the ones the OpenTelemetry spec's Benchmarking
    # section requires. OpenTelemetry cannot be reconfigured once its SDK has
    # been installed into a process, and the "api-only" configuration
    # requires that the SDK was never loaded, so exactly one configuration
    # can be measured per process. Comparison runs one subprocess per
    # configuration.
    #
    # @api private
    class Configuration
      # The environment variable naming the configuration to run under.
      ENV_VAR = 'DRIVER_BENCH_CONFIGURATION'

      # The configuration every other one is compared against.
      BASELINE = 'off'

      # @return [ String ] the short name of the configuration, used to key
      #   results and to select the configuration from the environment.
      attr_reader :name

      # @return [ String ] a human-readable description of the configuration.
      attr_reader :description

      # @return [ Hash ] options to pass to every Mongo::Client the
      #   benchmarks construct.
      attr_reader :client_options

      # @param name [ String ] the short name of the configuration.
      # @param description [ String ] what the configuration measures.
      # @param otel [ Symbol ] how much of OpenTelemetry to load: +:none+ for
      #   nothing, +:api+ for the API without an SDK, +:sdk+ for a configured
      #   SDK.
      # @param client_options [ Hash ] options for every client.
      # @param sampler_env [ Hash ] OpenTelemetry environment variables that
      #   select the sampler, applied before the SDK is configured.
      def initialize(name:, description:, otel:, client_options: {}, sampler_env: {})
        @name = name
        @description = description
        @otel = otel
        @client_options = client_options
        @sampler_env = sampler_env
      end

      # The test name to report a task's results under. perf.send only
      # accepts integer args, so the configuration goes into the name. The
      # baseline keeps the bare task name, so that its series continues the
      # one recorded before configurations existed.
      #
      # @param task [ String ] the micro-benchmark name.
      #
      # @return [ String ] the reported test name.
      def perf_test_name(task)
        baseline? ? task : "#{task} [#{name}]"
      end

      # The inverse of #perf_test_name.
      #
      # @param test_name [ String ] a reported test name.
      #
      # @return [ String ] the micro-benchmark name.
      def task_name(test_name)
        test_name.delete_suffix(" [#{name}]")
      end

      # Whether this configuration is the baseline that others are compared
      # against.
      def baseline?
        name == BASELINE
      end

      # Loads and configures OpenTelemetry for this configuration.
      #
      # Must run before any Mongo::Client is constructed: a client decides
      # whether tracing is active when it builds its tracer, and that
      # decision depends on whether ::OpenTelemetry is defined.
      def install!
        case @otel
        when :api then require 'opentelemetry-api'
        when :sdk then install_sdk!
        end
      end

      # All configurations, baseline first.
      ALL = [
        new(name: 'off',
            description: 'tracing disabled; the baseline',
            otel: :none),

        new(name: 'api-only',
            description: 'tracing enabled; OpenTelemetry API without an SDK, so every tracing call is a no-op',
            otel: :api,
            client_options: { tracing: { enabled: true } }),

        new(name: 'sdk-ratio',
            description: 'tracing enabled; SDK installed; TraceIdRatioBased sampler recording 1% of traces',
            otel: :sdk,
            client_options: { tracing: { enabled: true } },
            sampler_env: { 'OTEL_TRACES_SAMPLER' => 'traceidratio',
                           'OTEL_TRACES_SAMPLER_ARG' => '0.01' }),

        new(name: 'sdk-always',
            description: 'tracing enabled; SDK installed; every span sampled',
            otel: :sdk,
            client_options: { tracing: { enabled: true } },
            sampler_env: { 'OTEL_TRACES_SAMPLER' => 'always_on' })
      ].freeze

      # Looks up a configuration by name.
      #
      # @param name [ String ] the configuration name.
      #
      # @return [ Configuration ] the named configuration.
      #
      # @raise [ ArgumentError ] if no such configuration exists.
      def self.[](name)
        ALL.find { |configuration| configuration.name == name } ||
          raise(ArgumentError,
                "unknown configuration #{name.inspect}; " \
                "known configurations are #{ALL.map(&:name).join(', ')}")
      end

      # Fails when YJIT was asked for (RUBY_YJIT_ENABLE) but this ruby runs
      # without it. A ruby built without YJIT only warns and carries on, and
      # the benchmark would then quietly measure the interpreter, where the
      # cost of tracing is several times higher.
      def self.check_jit!
        return unless %w[1 true yes].include?(ENV['RUBY_YJIT_ENABLE'].to_s.downcase)
        return if defined?(RubyVM::YJIT) && RubyVM::YJIT.enabled?

        raise "RUBY_YJIT_ENABLE is set, but #{RUBY_DESCRIPTION} runs without YJIT"
      end

      # The configuration the current process is measuring, named by the
      # DRIVER_BENCH_CONFIGURATION environment variable.
      #
      # @return [ Configuration ] the current configuration.
      def self.current
        @current ||= self[ENV[ENV_VAR] || BASELINE]
      end

      private

      def install_sdk!
        require 'opentelemetry-sdk'

        # The SDK reads its sampler and exporter from the environment when it
        # is configured, so set them here rather than trusting the caller to:
        # a configuration must mean the same thing however the suite was
        # invoked.
        @sampler_env.each { |key, value| ENV[key] = value }

        # No exporter, and so no span processor either, as the spec
        # recommends. Sampled spans are still fully recorded without one; a
        # processor would charge the SDK's export machinery to the driver and
        # add variance.
        ENV['OTEL_TRACES_EXPORTER'] = 'none'

        ::OpenTelemetry::SDK.configure
      end
    end
  end
end

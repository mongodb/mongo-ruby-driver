# frozen_string_literal: true
# rubocop:todo all

require 'runners/cmap/verifier'

module Mongo
  module Cmap

    # Represents a specification.
    class Spec

      # @return [ String ] description The spec description.
      attr_reader :description

      # @return [ Hash ] pool_options The options for the created pools.
      attr_reader :pool_options

      # @return [ Array<Operation> ] spec_ops The spec operations.
      attr_reader :spec_ops

      # @return [ Error | nil ] error The expected error.
      attr_reader :expected_error

      # @return [ Array<Event::Base> ] events The events expected to occur.
      attr_reader :expected_events

      # @return [ Array<String> ] events The names of events to ignore.
      attr_reader :ignore_events

      # @return [ Mongo::ConnectionPool ] pool The connection pool to use for operations.
      attr_reader :pool

      # @return [ Mrss::EventSubscriber ] subscriber The subscriber receiving the CMAP events.
      attr_reader :subscriber

      # Instantiate the new spec.
      #
      # @param [ String ] test_path The path to the file.
      def initialize(test_path)
        @test = ::Utils.load_spec_yaml_file(test_path)

        @description = @test['description']
        @pool_options = process_options(@test['poolOptions'])
        @spec_ops = @test['operations'].map { |o| Operation.new(self, o) }
        @expected_error = @test['error']
        @expected_events = @test['events']
        @ignore_events = @test['ignore'] || []
        @fail_point_command = @test['failPoint']
        @threads = Set.new

        process_run_on
      end

      attr_reader :pool

      def setup(server, client, subscriber)
        @subscriber = subscriber
        @client = client
        # The driver always creates pools for known servers.
        # There is a test which creates and destroys a pool and it only expects
        # those two events, not the ready event.
        # This situation cannot happen in normal driver operation, but to
        # support this test, create the pool manually here.
        @pool = Mongo::Server::ConnectionPool.new(server, server.options)
        server.instance_variable_set(:@pool, @pool)

        configure_fail_point
      end

      def run
        state = {}

        {}.tap do |result|
          spec_ops.each do |op|
            err = op.run(pool, state)

            if err
              result['error'] = err
              break
            elsif op.name == 'start'
              @threads << state[op.target]
            end
          end

          result['error'] ||= nil
          result['events'] = subscriber.published_events.each_with_object([]) do |event, events|
            next events unless event.is_a?(Mongo::Monitoring::Event::Cmap::Base)

            event = case event
                    when Mongo::Monitoring::Event::Cmap::PoolCreated
                      {
                        'type' => 'ConnectionPoolCreated',
                        'address' => event.address,
                        'options' => normalize_options(event.options),
                      }
                    when Mongo::Monitoring::Event::Cmap::PoolClosed
                      {
                        'type' => 'ConnectionPoolClosed',
                        'address' => event.address,
                      }
                    when Mongo::Monitoring::Event::Cmap::ConnectionCreated
                      {
                        'type' => 'ConnectionCreated',
                        'connectionId' => event.connection_id,
                        'address' => event.address,
                      }
                    when Mongo::Monitoring::Event::Cmap::ConnectionReady
                      {
                        'type' => 'ConnectionReady',
                        'connectionId' => event.connection_id,
                        'address' => event.address,
                      }
                    when Mongo::Monitoring::Event::Cmap::ConnectionClosed
                      {
                        'type' => 'ConnectionClosed',
                        'connectionId' => event.connection_id,
                        'reason' => event.reason,
                        'address' => event.address,
                      }
                    when Mongo::Monitoring::Event::Cmap::ConnectionCheckOutStarted
                      {
                        'type' => 'ConnectionCheckOutStarted',
                        'address' => event.address,
                      }
                    when Mongo::Monitoring::Event::Cmap::ConnectionCheckOutFailed
                      {
                        'type' => 'ConnectionCheckOutFailed',
                        'reason' => event.reason,
                        'address' => event.address,
                      }
                   when Mongo::Monitoring::Event::Cmap::ConnectionCheckedOut
                      {
                        'type' => 'ConnectionCheckedOut',
                        'connectionId' => event.connection_id,
                        'address' => event.address,
                      }
                    when Mongo::Monitoring::Event::Cmap::ConnectionCheckedIn
                      {
                        'type' => 'ConnectionCheckedIn',
                        'connectionId' => event.connection_id,
                        'address' => event.address,
                      }
                    when Mongo::Monitoring::Event::Cmap::PoolCleared
                      {
                        'type' => 'ConnectionPoolCleared',
                        'address' => event.address,
                        'interruptInUseConnections' => event.options[:interrupt_in_use_connections]
                      }
                    when Mongo::Monitoring::Event::Cmap::PoolReady
                      {
                        'type' => 'ConnectionPoolReady',
                        'address' => event.address,
                      }
                    else
                      raise "Unhandled event: #{event}"
                    end

            events << event unless @ignore_events.include?(event.fetch('type'))
          end
        end
      ensure
        disable_fail_points
        kill_remaining_threads
      end

      def disable_fail_points
        if @fail_point_command
          @client.command(
            configureFailPoint: @fail_point_command['configureFailPoint'],
            mode: 'off'
          )
        end
      end

      def kill_remaining_threads
        @threads.each(&:kill)
      end

      def satisfied?
        cc = ClusterConfig.instance
        ok = true
        if @min_server_version
          ok &&= Gem::Version.new(cc.fcv_ish) >= Gem::Version.new(@min_server_version)
        end
        if @max_server_version
          ok &&= Gem::Version.new(cc.server_version) <= Gem::Version.new(@max_server_version)
        end
        if @topologies
          ok &&= @topologies.include?(cc.topology)
        end
        if @oses
          ok &&= @oses.any? { |os| SpecConfig.instance.send("#{os.to_s}?")}
        end
        ok
      end

      private

      # Converts the options used by the Ruby driver to the spec test format.
      def normalize_options(options)
        (options || {}).reduce({}) do |opts, kv|
         case kv.first
          when :max_idle_time
            opts['maxIdleTimeMS'] = (kv.last * 1000.0).to_i
          when :max_size
            opts['maxPoolSize'] = kv.last
          when :min_size
            opts['minPoolSize'] = kv.last
          when :wait_queue_size
            opts['waitQueueSize'] = kv.last
          when :wait_timeout
            opts['waitQueueTimeoutMS'] = (kv.last * 1000.0).to_i
          end

          opts
        end
      end

      # Converts the options given by the spec to the Ruby driver format.
      #
      # This method only handles options used by spec tests at the time when
      # this method was written. Other options are silently dropped.
      def process_options(options)
        (options || {}).each_with_object({}) do |kv, opts|
          case kv.first
          when 'maxIdleTimeMS'
            opts[:max_idle_time] = kv.last / 1000.0
          when 'maxPoolSize'
            opts[:max_pool_size] = kv.last
          when 'minPoolSize'
            opts[:min_pool_size] = kv.last
          when 'waitQueueSize'
            opts[:wait_queue_size] = kv.last
          when 'waitQueueTimeoutMS'
            opts[:wait_queue_timeout] = kv.last / 1000.0
          when 'backgroundThreadIntervalMS'
            # The populator busy loops, this option doesn't apply to our driver.
          when 'maxConnecting'
            opts[:max_connecting] = kv.last
          when 'appName'
            opts[:app_name] = kv.last
          else
            raise "Unknown option #{kv.first}"
          end
        end
      end

      def process_run_on
        if run_on = @test['runOn']
          @min_server_version = run_on.detect do |doc|
            doc.keys.first == 'minServerVersion'
          end&.values&.first
          @max_server_version = run_on.detect do |doc|
            doc.keys.first == 'maxServerVersion'
          end&.values&.first

          @topologies = if topologies = run_on.detect { |doc| doc.keys.first == 'topology' }
            (topologies['topology'] || {}).map do |topology|
              {
                'replicaset' => :replica_set,
                'single' => :single,
                'sharded' => :sharded,
                'sharded-replicaset' => :sharded,
                'load-balanced' => :load_balanced,
              }[topology].tap do |v|
                unless v
                  raise "Unknown topology #{topology}"
                end
              end
            end
          end

          @oses = if oses = run_on.detect { |doc| doc.keys.first == 'requireOs' }
            (oses['requireOs'] || {}).map do |os|
              {
                'macos' => :macos,
                'linux' => :linux,
                'windows' => :windows,
              }[os].tap do |v|
                unless v
                  raise "Unknown os #{os}"
                end
              end
            end
          end
        end
      end

      def configure_fail_point
        @client.database.command(@fail_point_command) if @fail_point_command
      end
    end

    # Represents an operation in the spec. Operations are sequential.
    class Operation
      include RSpec::Mocks::ExampleMethods

      # @return [ String ] command The name of the operation to run.
      attr_reader :name

      # @return [ String | nil ] thread The identifier of the thread to run the operation on (`nil`
      #   signifying the default thread.)
      attr_reader :thread

      # @return [ String | nil ] target The name of the started thread.
      attr_reader :target

      # @return [ Integer | nil ] ms The number of milliseconds to sleep.
      attr_reader :ms

      # @return [ String | nil ] label The label for the returned connection.
      attr_reader :label

      # @return [ true | false ] interrupt_in_use_connections Whether or not
      #   all connections should be closed on pool clear.
      attr_reader :interrupt_in_use_connections

      # @return [ String | nil ] The binding for the connection which should run the operation.
      attr_reader :connection

      # @return [ Mongo::ConnectionPool ] pool The connection pool to use for the operation.
      attr_reader :pool

      # Create the new Operation.
      #
      # @param [ Spec ] spec The Spec object.
      # @param [ Hash ] operation The operation hash.
      def initialize(spec, operation)
        @spec = spec
        @name = operation['name']
        @thread = operation['thread']
        @target = operation['target']
        @ms = operation['ms']
        @label = operation['label']
        @connection = operation['connection']
        @event = operation['event']
        @count = operation['count']
        @interrupt_in_use_connections = !!operation['interruptInUseConnections']
      end

      def run(pool, state, main_thread = true)
        return run_on_thread(state) if thread && main_thread

        @pool = pool
        case name
        when 'start'
          run_start_op(state)
        when 'ready'
          run_ready_op(state)
        when 'wait'
          run_wait_op(state)
        when 'waitForThread'
          run_wait_for_thread_op(state)
        when 'waitForEvent'
          run_wait_for_event_op(state)
        when 'checkOut'
          run_checkout_op(state)
        when 'checkIn'
          run_checkin_op(state)
        when 'clear'
          run_clear_op(state)
        when 'close'
          run_close_op(state)
        else
          raise "invalid operation: #{name}"
        end

        nil

      # We hard-code the error messages because ours contain information like the address and the
      # connection ID.
      rescue Error::PoolClosedError
        raise unless main_thread

        {
          'type' => 'PoolClosedError',
          'message' => 'Attempted to check out a connection from closed connection pool',
        }
      rescue Error::ConnectionCheckOutTimeout
        raise unless main_thread

        {
          'type' => 'WaitQueueTimeoutError',
          'message' => 'Timed out while checking out a connection from connection pool',
        }
      end

      private

      def run_start_op(state)
        thread_context = ThreadContext.new
        thread = Thread.start do
          loop do
            begin
              op = thread_context.operations.pop(true)
              op.run(pool, state, false)
            rescue ThreadError
              # Queue is empty
            end
            if thread_context.stop?
              break
            else
              sleep 0.1
            end
          end
        end
        class << thread
          attr_accessor :context
        end
        thread.context = thread_context
        state[target] = thread

        # Allow the thread to begin running.
        sleep 0.1

        # Since we expect exceptions to occur in some cases, we disable the printing of error
        # messages from the thread if the Ruby version supports it.
        if state[target].respond_to?(:report_on_exception)
          state[target].report_on_exception = false
        end
      end

      def run_wait_op(_state)
        sleep(ms / 1000.0)
      end

      def run_wait_for_thread_op(state)
        if thread = state[target]
          thread.context.signal_stop
          thread.join
        else
          raise "Expected thread for '#{thread}' but none exists."
        end
        nil
      end

      def run_wait_for_event_op(state)
        subscriber = @spec.subscriber
        looped = 0
        deadline = Utils.monotonic_time + 3
        loop do
          actual_events = @spec.subscriber.published_events.select do |e|
            e.class.name.sub(/.*::/, '').sub(/^ConnectionPool/, 'Pool') == @event.sub(/^ConnectionPool/, 'Pool')
          end
          if actual_events.length >= @count
            break
          end
          if looped == 1
            puts("Waiting for #{@count} #{@event} events (have #{actual_events.length}): #{@spec.description}")
          end
          if Utils.monotonic_time > deadline
            raise "Did not receive #{@count} #{@event} events in time (have #{actual_events.length}): #{@spec.description}"
          end
          looped += 1
          sleep 0.1
        end
      end

      def run_checkout_op(state)
        conn = pool.check_out
        state[label] = conn if label
      end

      def run_checkin_op(state)
        until state[connection]
          sleep(0.2)
        end

        pool.check_in(state[connection])
      end

      def run_clear_op(state)
        RSpec::Mocks.with_temporary_scope do
          allow(pool.server).to receive(:unknown?).and_return(true)

          pool.clear(lazy: true, interrupt_in_use_connections: interrupt_in_use_connections)
        end
      end

      def run_close_op(state)
        pool.close
      end

      def run_ready_op(state)
        pool.ready
      end

      def run_on_thread(state)
        if thd = state[thread]
          thd.context.operations << self
          # Sleep to allow the other thread to execute the new command.
          sleep 0.1
        else
          raise "Expected thread for '#{thread}' but none exists."
        end
        nil
      end
    end

    class ThreadContext
      def initialize
        @operations = Queue.new
      end

      def stop?
        !!@stop
      end

      def signal_stop
        @stop = true
      end

      attr_reader :operations
    end
  end
end

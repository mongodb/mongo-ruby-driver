require 'support/cmap/verifier'

module Mongo
  module Cmap

    # Represents a specification.
    #
    # @since 2.8.0
    class Spec

      # @return [ String ] description The spec description.
      attr_reader :description

      # @return [ Hash ] pool_options The options for the created pools.
      attr_reader :pool_options

      # @return [ Array<Operation> ] spec_ops The spec operations.
      attr_reader :spec_ops

      # @return [ Array<Operation> ] processed_ops The processed operations.
      attr_reader :processed_ops

      # @return [ Error | nil ] error The expected error.
      attr_reader :expected_error

      # @return [ Array<Event::Base> ] events The events expected to occur.
      attr_reader :expected_events

      # @return [ Array<String> ] events The names of events to ignore.
      attr_reader :ignore_events

      # @return [ Mongo::ConnectionPool ] pool The connection pool to use for operations.
      attr_reader :pool

      # @return [ EventSubscriber ] subscriber The subscriber receiving the CMAP events.
      attr_reader :subscriber

      # Instantiate the new spec.
      #
      # @example Create the spec.
      #   Spec.new(file)
      #
      # @param [ String ] file The name of the file.
      #
      # @since 2.8.0
      def initialize(file)
        @test = YAML.load(ERB.new(File::read(file)).result)

        @description = @test['description']
        @pool_options = snakeize_hash(process_options(@test['poolOptions']))
        @spec_ops = @test['operations'].map { |o| Operation.new(o) }
        @processed_ops = []
        @expected_error = @test['error']
        @expected_events = @test['events']
        @ignore_events = @test['ignore'] || []
      end

      def setup(cluster)
        @subscriber = EventSubscriber.new

        monitoring = Mongo::Monitoring.new(monitoring: false)
        monitoring.subscribe(Mongo::Monitoring::CONNECTION_POOL, subscriber)

        server = Mongo::Server.new(
            Address.new(SpecConfig.instance.addresses.first),
            cluster,
            monitoring,
            Mongo::Event::Listeners.new,
            pool_options.merge(monitoring_io: false))

        @pool = Mongo::Server::ConnectionPool.get(server) do
          Mongo::Server::Connection.new(server)
        end

        preprocess
      end

      def run
        state = {}

        {}.tap do |result|
          processed_ops.each do |op|
            err = op.run(pool, state)

            if err
              result['error'] = err
              break
            end
          end

          result['error'] ||= nil
          result['events'] = subscriber.succeeded_events.reduce([]) do |events, event|
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
                      }
                    when Mongo::Monitoring::Event::Cmap::ConnectionReady
                      {
                        'type' => 'ConnectionReady',
                        'connectionId' => event.connection_id,
                      }
                    when Mongo::Monitoring::Event::Cmap::ConnectionClosed
                      {
                        'type' => 'ConnectionClosed',
                        'connectionId' => event.connection_id,
                        'reason' => event.reason,
                      }
                    when Mongo::Monitoring::Event::Cmap::ConnectionCheckoutStarted
                      {
                        'type' => 'ConnectionCheckOutStarted',
                    }
                    when Mongo::Monitoring::Event::Cmap::ConnectionCheckoutFailed
                      {
                        'type' => 'ConnectionCheckOutFailed',
                        'reason' => event.reason,
                      }
                   when Mongo::Monitoring::Event::Cmap::ConnectionCheckedOut
                      {
                        'type' => 'ConnectionCheckedOut',
                        'connectionId' => event.connection_id,
                      }
                    when Mongo::Monitoring::Event::Cmap::ConnectionCheckedIn
                      {
                        'type' => 'ConnectionCheckedIn',
                        'connectionId' => event.connection_id,
                      }
                    when Mongo::Monitoring::Event::Cmap::PoolCleared
                      {
                        'type' => 'ConnectionPoolCleared',
                        'address' => event.address,
                      }
                    end

            events << event unless @ignore_events.include?(event['type'])
            events
          end
        end
      end

      private

      # Converts the options used by the Ruby driver to the spec test format.
      def normalize_options(options)
        (options || {}).reduce({}) do |opts, kv|
         case kv.first
          when :max_idle_time
            opts['maxIdleTimeMS'] = (kv.last * 1000.0).to_i
          when :max_pool_size
            opts['maxPoolSize'] = kv.last
          when :min_pool_size
            opts['minPoolSize'] = kv.last
          when :wait_queue_size
            opts['waitQueueSize'] = kv.last
          when :wait_queue_timeout
            opts['waitQueueTimeoutMS'] = (kv.last * 1000.0).to_i
          end

          opts
        end
      end

      # Converts the options given by the spec to the Ruby driver format.
      def process_options(options)
        (options || {}).reduce({}) do |opts, kv|
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
          end

          opts
        end
      end

      # Places operations run by the non-main thread in the `thread_ops` field of the corresponding
      # `start` operation.
      def preprocess
        until spec_ops.empty?
          processed_ops << spec_ops.shift

          if processed_ops.last.name == "start"
            spec_ops.delete_if do |op|
              if op.thread == processed_ops.last.target
                processed_ops.last.thread_ops << op
              end
            end
          end
        end
      end
    end

    # Represents an operation in the spec. Operations are sequential.
    #
    # @since 2.8.0
    class Operation

      # @return [ String ] command The name of the operation to run.
      attr_reader :name

      # @return [ String | nil ] thread The identifier of the thread to run the operation on (`nil`
      #   signifying the default thread.)
      attr_reader :thread

      # @return [ String | nil ] target The name of the started thread.
      attr_reader :target

      # @return [ Array<Operation> ] thread_ops The operations to run on the thread.
      attr_reader :thread_ops

      # @return [ Integer | nil ] ms The number of milliseconds to sleep.
      attr_reader :ms

      # @return [ String | nil ] label The label for the returned connection.
      attr_reader :label

      # @return [ String | nil ] The binding for the connection which should run the operation.
      attr_reader :connection

      # @return [ Mongo::ConnectionPool ] pool The connection pool to use for the operation.
      attr_reader :pool

      # Create the new Operation.
      #
      # @example Create the new Operation.
      #   Operation.new(operation)
      #
      # @param [ Hash ] operation The operation hash.
      #
      # @since 2.8.0
      def initialize(operation)
        @name = operation['name']
        @thread = operation['thread']
        @thread_ops = []
        @target = operation['target']
        @ms = operation['ms']
        @label = operation['label']
        @connection = operation['connection']
      end

      def run(pool, state, main_thread = true)
        @pool = pool

        case name
        when 'start'
          run_start_op(state)
        when 'wait'
          run_wait_op(state)
        when 'waitFor'
          run_wait_for_op(state)
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
      rescue Error::PoolClosed
        raise unless main_thread

        {
          'type' => 'PoolClosedError',
          'message' => 'Attempted to check out a connection from closed connection pool',
        }
      rescue Error::ConnectionCheckoutTimeout
        raise unless main_thread

        {
          'type' => 'WaitQueueTimeoutError',
          'message' => 'Timed out while checking out a connection from connection pool',
        }
      end

      private

      def run_start_op(state)
        state[target] = Thread.start do
          Thread.current[:name] = @target
          thread_ops.each { |op| op.run(pool, state, false) }
        end

        # Since we expect exceptions to occur in some cases, we disable the printing of error
        # messages from the thread if the Ruby version supports it.
        if state[target].respond_to?(:report_on_exception)
          state[target].report_on_exception = false
        end
      end

      def run_wait_op(_state)
        sleep(ms / 1000.0)
      end

      def run_wait_for_op(state)
        state[target].join
      end

      def run_checkout_op(state)
        conn = pool.checkout
        state[label] = conn if label
      end

      def run_checkin_op(state)
        until state[connection]
          sleep(0.2)
        end

        pool.checkin(state[connection])
      end

      def run_clear_op(state)
        pool.clear
      end

      def run_close_op(state)
        pool.close!
      end
    end
  end
end

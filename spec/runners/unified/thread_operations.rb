# frozen_string_literal: true
# rubocop:todo all

module Unified

  module ThreadOperations

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

    def wait(op)
      consume_test_runner(op)
      use_arguments(op) do |args|
        sleep args.use!('ms') / 1000.0
      end
    end

    def wait_for_event(op)
      consume_test_runner(op)
      use_arguments(op) do |args|
        client = entities.get(:client, args.use!('client'))
        subscriber = @subscribers.fetch(client)
        event = args.use!('event')
        assert_eq(event.keys.length, 1, "Expected event must have one key: #{event}")
        count = args.use!('count')

        deadline = Mongo::Utils.monotonic_time + 10
        loop do
          events = select_events(subscriber, event)
          if events.length >= count
            break
          end
          if Mongo::Utils.monotonic_time >= deadline
            raise "Did not receive an event matching #{event} in 10 seconds; received #{events.length} but expected #{count} events"
          else
            sleep 0.1
          end
        end
      end
    end

    def run_on_thread(op)
      consume_test_runner(op)
      use_arguments(op) do |args|
        thread = entities.get(:thread, args.use!('thread'))
        operation = args.use!('operation')
        thread.context.operations << operation
      end
    end

    def wait_for_thread(op)
      consume_test_runner(op)
      use_arguments(op) do |args|
        thread = entities.get(:thread, args.use!('thread'))
        thread.context.signal_stop
        thread.join
      end
    end
  end
end

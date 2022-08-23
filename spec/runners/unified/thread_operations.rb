# frozen_string_literal: true
# encoding: utf-8

module Unified

  module ThreadOperations

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
        timeout = args.use('timeout') || 5

        deadline = Time.now + timeout
        loop do
          events = select_events(subscriber, event)
          if events.length >= count
            break
          end
          if Time.now >= deadline
            raise "Did not receive an event matching #{event} in 5 seconds; received #{events.length} but expected #{count} events"
          else
            sleep 0.1
          end
        end
      end
    end
  end
end
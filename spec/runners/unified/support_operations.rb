# frozen_string_literal: true
# rubocop:todo all

module Unified

  module SupportOperations

    def run_command(op)
      database = entities.get(:database, op.use!('object'))

      use_arguments(op) do |args|
        args.use!('commandName')

        cmd = args.use!('command')

        opts = {}
        if session = args.use('session')
          opts[:session] = entities.get(:session, session)
        end
        if read_preference = args.use('readPreference')
          opts[:read] = ::Utils.snakeize_hash(read_preference)
        end

        database.command(cmd, **opts)
      end
    end

    def fail_point(op)
      consume_test_runner(op)
      use_arguments(op) do |args|
        client = entities.get(:client, args.use!('client'))
        client.command(fp = args.use('failPoint'))

        $disable_fail_points ||= []
        $disable_fail_points << [
          fp,
          ClusterConfig.instance.primary_address,
        ]
      end
    end

    def targeted_fail_point(op)
      consume_test_runner(op)
      use_arguments(op) do |args|
        session = args.use!('session')
        session = entities.get(:session, session)
        unless session.pinned_server
          raise ArgumentError, 'Targeted fail point requires session to be pinned to a server'
        end

        client = ClusterTools.instance.direct_client(session.pinned_server.address,
          database: 'admin')
        client.command(fp = args.use!('failPoint'))
        args.clear

        $disable_fail_points ||= []
        $disable_fail_points << [
          fp,
          session.pinned_server.address,
        ]
      end
    end

    def end_session(op)
      session = entities.get(:session, op.use!('object'))
      session.end_session
    end

    def assert_session_dirty(op)
      consume_test_runner(op)
      use_arguments(op) do |args|
        session = entities.get(:session, args.use!('session'))
        # https://jira.mongodb.org/browse/RUBY-1813
        true
      end
    end

    def assert_session_not_dirty(op)
      consume_test_runner(op)
      use_arguments(op) do |args|
        session = entities.get(:session, args.use!('session'))
        # https://jira.mongodb.org/browse/RUBY-1813
        true
      end
    end

    def assert_same_lsid_on_last_two_commands(op, expected: true)
      consume_test_runner(op)
      use_arguments(op) do |args|
        client = entities.get(:client, args.use!('client'))
        subscriber = @subscribers.fetch(client)
        unless subscriber.started_events.length >= 2
          raise Error::ResultMismatch, "Must have at least 2 events, have #{subscriber.started_events.length}"
        end
        lsids = subscriber.started_events[-2...-1].map do |cmd|
          cmd.command.fetch('lsid')
        end
        if expected
          unless lsids.first == lsids.last
            raise Error::ResultMismatch, "lsids differ but they were expected to be the same"
          end
        else
          if lsids.first == lsids.last
            raise Error::ResultMismatch, "lsids are the same but they were expected to be different"
          end
        end
      end
    end

    def assert_different_lsid_on_last_two_commands(op)
      assert_same_lsid_on_last_two_commands(op, expected: false)
    end

    def start_transaction(op)
      $klil_transactions = true
      session = entities.get(:session, op.use!('object'))
      assert_no_arguments(op)
      session.start_transaction
    end

    def assert_session_transaction_state(op)
      consume_test_runner(op)
      use_arguments(op) do |args|
        session = entities.get(:session, args.use!('session'))
        state = args.use!('state')
        unless session.send("#{state}_transaction?")
          raise Error::ResultMismatch, "Expected session to have state #{state}"
        end
      end
    end

    def commit_transaction(op)
      session = entities.get(:session, op.use!('object'))
      assert_no_arguments(op)
      session.commit_transaction
    end

    def abort_transaction(op)
      session = entities.get(:session, op.use!('object'))
      assert_no_arguments(op)
      session.abort_transaction
    end

    def with_transaction(op)
      $kill_transactions = true
      session = entities.get(:session, op.use!('object'))
      use_arguments(op) do |args|
        ops = args.use!('callback')

        if args.empty?
          opts = {}
        else
          opts = ::Utils.underscore_hash(args)
          if value = opts[:read_concern]&.[](:level)
            opts[:read_concern][:level] = value.to_sym
          end
          args.clear
        end

        session.with_transaction(**opts) do
          execute_operations(ops)
        end
      end
    end

    def assert_session_pinned(op, state = true)
      consume_test_runner(op)
      use_arguments(op) do |args|
        session = entities.get(:session, args.use!('session'))

        if state
          unless session.pinned_server
            raise Error::ResultMismatch, 'Expected session to be pinned but it is not'
          end
        else
          if session.pinned_server
            raise Error::ResultMismatch, 'Expected session to be not pinned but it is'
          end
        end
      end
    end

    def assert_session_unpinned(op)
      assert_session_pinned(op, false)
    end

    def _loop(op)
      consume_test_runner(op)
      use_arguments(op) do |args|
        ops = args.use!('operations')

        if store_errors = args.use('storeErrorsAsEntity')
          entities.set(:error_list, store_errors, [])
        end

        if store_failures = args.use('storeFailuresAsEntity')
          entities.set(:failure_list, store_failures, [])
        end

        store_iterations = args.use('storeIterationsAsEntity')
        iterations = 0
        store_successes = args.use('storeSuccessesAsEntity')
        successes = 0

        loop do
          break if stop?
          begin
            ops.map(&:dup).each do |op|
              execute_operation(op)
              successes += 1
            end
          rescue Unified::Error::ResultMismatch => e
            if store_failures
              STDERR.puts "Failure: #{e.class}: #{e}"
              entities.get(:failure_list, store_failures) << {
                error: "#{e.class}: #{e}",
                time: Time.now.to_f,
              }
            elsif store_errors
              STDERR.puts "Failure: #{e.class}: #{e} (reporting as error)"
              entities.get(:error_list, store_errors) << {
                error: "#{e.class}: #{e}",
                time: Time.now.to_f,
              }
            else
              raise
            end
          rescue Interrupt
            raise
          rescue => e
            if store_failures
              STDERR.puts "Error: #{e.class}: #{e} (reporting as failure)"
              entities.get(:failure_list, store_failures) << {
                error: "#{e.class}: #{e}",
                time: Time.now.to_f,
              }
            elsif store_errors
              STDERR.puts "Error: #{e.class}: #{e}"
              entities.get(:error_list, store_errors) << {
                error: "#{e.class}: #{e}",
                time: Time.now.to_f,
              }
            else
              raise
            end
          end
          iterations += 1
        end

        if store_iterations
          entities.set(:iteration_count, store_iterations, iterations)
        end
        if store_successes
          entities.set(:success_count, store_successes, successes)
        end
      end
    end

    def assert_event_count(op)
      consume_test_runner(op)
      use_arguments(op) do |args|
        client = entities.get(:client, args.use!('client'))
        subscriber = @subscribers.fetch(client)
        event = args.use!('event')
        assert_eq(event.keys.length, 1, "Expected event must have one key: #{event}")
        count = args.use!('count')

        events = select_events(subscriber, event)
        if %w(serverDescriptionChangedEvent poolClearedEvent).include?(event.keys.first)
          # We publish SDAM events from both regular and push monitors.
          # This means sometimes there are two ServerMarkedUnknownEvent
          # events published for the same server transition.
          # Allow actual event count to be at least the expected event count
          # in case there are multiple transitions in a single test.
          assert_gte(events.length, count, "Expected event #{event} to occur #{count} times but received it #{events.length} times.")
        else
          assert_eq(events.length, count, "Expected event #{event} to occur #{count} times but received it #{events.length} times.")
        end
      end
    end

    def select_events(subscriber, event)
      expected_name, opts = event.first
      expected_name = expected_name.sub(/Event$/, '').sub(/^(.)/) { $1.upcase }
      subscriber.wanted_events.select do |wevent|
        if wevent.class.name.sub(/.*::/, '') == expected_name
          spec = UsingHash[opts]
          result = true
          if new_desc = spec.use('newDescription')
            if type = new_desc.use('type')
              result &&= wevent.new_description.server_type == type.downcase.to_sym
            end
          end
          unless spec.empty?
            raise NotImplementedError, "Unhandled keys: #{spec}"
          end
          result
        end
      end
    end

    def assert_number_connections_checked_out(op)
      consume_test_runner(op)
      use_arguments(op) do |args|
        client = entities.get(:client, args.use!('client'))
        connections = args.use!('connections')
        actual_c = client.cluster.servers.map(&:pool_internal).compact.sum do |p|
          p.instance_variable_get(:@checked_out_connections).length
        end
        assert_eq(actual_c, connections, "Expected client #{client} to have #{connections} checked out connections but there are #{actual_c}.")
      end
    end

    private

    def assert_no_arguments(op)
      if op.key?('arguments')
        raise NotimplementedError, "Arguments are not allowed"
      end
    end

    def consume_test_runner(op)
      v = op.use!('object')
      unless v == 'testRunner'
        raise NotImplementedError, 'Expected object to be testRunner'
      end
    end

    def decode_hex_bytes(value)
      value.scan(/../).map { |hex| hex.to_i(16).chr }.join
    end
  end
end

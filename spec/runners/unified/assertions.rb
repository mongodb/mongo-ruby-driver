# frozen_string_literal: true
# rubocop:todo all

module Unified

  module Assertions
    include RSpec::Matchers

    def assert_result_matches(actual, expected)
      if Hash === expected
        if expected.keys == ["$$unsetOrMatches"]
          assert_result_matches(actual, UsingHash[expected.values.first])
        else
          use_all(expected, 'expected result', expected) do |expected|
            %w(deleted inserted matched modified upserted).each do |k|
              if count = expected.use("#{k}Count")
                if Hash === count || count > 0
                  actual_count = case actual
                  when Mongo::BulkWrite::Result, Mongo::Operation::Delete::Result, Mongo::Operation::Update::Result
                    actual.send("#{k}_count")
                  else
                    actual["n_#{k}"]
                  end
                  assert_value_matches(actual_count, count, "#{k} count")
                end
              end
            end
            %w(inserted upserted).each do |k|
              expected_v = expected.use("#{k}Ids")
              next unless expected_v
              actual_v = case actual
              when Mongo::BulkWrite::Result, Mongo::Operation::Update::Result
                # Ruby driver returns inserted ids as an array of ids.
                # The yaml file specifies them as a map from operation.
                if Hash === expected_v && expected_v.keys == %w($$unsetOrMatches)
                  expected_v = expected_v.values.first.values
                elsif Hash === expected_v
                  expected_v = expected_v.values
                end
                actual.send("#{k}_ids")
              else
                actual["#{k}_ids"]
              end
              if expected_v
                if expected_v.empty?
                  if actual_v && !actual_v.empty?
                    raise Error::ResultMismatch, "Actual not empty"
                  end
                else
                  if actual_v != expected_v
                    raise Error::ResultMismatch, "Mismatch: actual #{actual_v}, expected #{expected_v}"
                  end
                end
              end
            end

            %w(acknowledged).each do |k|
              expected_v = expected.use(k)
              next unless expected_v
              actual_v = case actual
              when Mongo::BulkWrite::Result, Mongo::Operation::Result
                if Hash === expected_v && expected_v.keys == %w($$unsetOrMatches)
                  expected_v = expected_v.values.first
                end
                actual.send("#{k}?")
              else
                actual[k]
              end
              if expected_v
                if expected_v.empty?
                  if actual_v && !actual_v.empty?
                    raise Error::ResultMismatch, "Actual not empty"
                  end
                else
                  if actual_v != expected_v
                    raise Error::ResultMismatch, "Mismatch: actual #{actual_v}, expected #{expected_v}"
                  end
                end
              end
            end

            %w(bulkWriteResult).each do |k|
              expected_v = expected.use(k)
              next unless expected_v
              actual_v = case actual
              when Mongo::Crypt::RewrapManyDataKeyResult
                actual.send(Utils.underscore(k))
              else
                raise Error::ResultMismatch, "Mismatch: actual #{actual_v}, expected #{expected_v}"
              end
              if expected_v
                if expected_v.empty?
                  if actual_v && !actual_v.empty?
                    raise Error::ResultMismatch, "Actual not empty"
                  end
                else
                  %w(deleted inserted matched modified upserted).each do |k|
                    if count = expected_v.use("#{k}Count")
                      if Hash === count || count > 0
                        actual_count = actual_v.send("#{k}_count")
                        assert_value_matches(actual_count, count, "#{k} count")
                      end
                    end
                  end
                end
              end
            end
            assert_matches(actual, expected, 'result')
            expected.clear
          end
        end
      else
        assert_matches(actual, expected, 'result')
      end
    end

    def assert_outcome
      return unless outcome

      client = ClientRegistry.instance.global_client('root_authorized')
      outcome.each do |spec|
        spec = UsingHash[spec]
        collection = client.use(spec.use!('databaseName'))[spec.use!('collectionName')]
        expected_docs = spec.use!('documents')
        actual_docs = collection.find({}, sort: { _id: 1 }).to_a
        assert_documents_match(actual_docs, expected_docs)
        unless spec.empty?
          raise NotImplementedError, "Unhandled keys: #{spec}"
        end
      end
    end

    def assert_documents_match(actual, expected)
      unless actual.length == expected.length
        raise Error::ResultMismatch, "Unexpected number of documents: expected #{expected.length}, actual #{actual.length}"
      end

      actual.each_with_index do |document, index|
        assert_matches(document, expected[index], "document ##{index}")
      end
    end

    def assert_document_matches(actual, expected, msg)
      unless actual == expected
        raise Error::ResultMismatch, "#{msg} does not match"
      end
    end

    def assert_events
      return unless @expected_events
      @expected_events.each do |spec|
        spec = UsingHash[spec]
        client_id = spec.use!('client')
        client = entities.get(:client, client_id)
        subscriber = @subscribers.fetch(client)
        expected_events = spec.use!('events')
        ignore_extra_events = if ignore = spec.use('ignoreExtraEvents')
          # Ruby treats 0 as truthy, whereas the spec tests use it as falsy.
          ignore == 0 ? false : ignore
        else
          false
        end
        actual_events = subscriber.wanted_events(@observe_sensitive[client_id])
        case spec.use('eventType')
        when nil, 'command'
          actual_events.select! do |event|
            event.class.name.sub(/.*::/, '') =~ /^Command/
          end
        when 'cmap'
          actual_events.select! do |event|
            event.class.name.sub(/.*::/, '') =~ /^(?:Pool|Connection)/
          end
        end

        if (!ignore_extra_events && actual_events.length != expected_events.length) ||
           (ignore_extra_events && actual_events.length < expected_events.length)
          raise Error::ResultMismatch, "Event count mismatch: expected #{expected_events.length}, actual #{actual_events.length}\nExpected: #{expected_events}\nActual: #{actual_events}"
        end
        expected_events.each_with_index do |event, i|
          assert_event_matches(actual_events[i], event)
        end
        unless spec.empty?
          raise NotImplementedError, "Unhandled keys: #{spec}"
        end
      end
    end

    def assert_event_matches(actual, expected)
      assert_eq(expected.keys.length, 1, "Expected event must have one key: #{expected}")
      expected_name, spec = expected.first
      spec = UsingHash[spec]
      expected_name = expected_name.sub(/Event$/, '').sub(/^(.)/) { $1.upcase }
      assert_eq(actual.class.name.sub(/.*::/, ''), expected_name, 'Event name does not match')
      if spec.use('hasServiceId')
        actual.service_id.should_not be nil
      end
      if spec.use('hasServerConnectionId')
        actual.server_connection_id.should_not be nil
      end
      if db_name = spec.use('databaseName')
        assert_eq(actual.database_name, db_name, 'Database names differ')
      end
      if command_name = spec.use('commandName')
        assert_eq(actual.command_name, command_name, 'Command names differ')
      end
      if command = spec.use('command')
        assert_matches(actual.command, command, 'Commands differ')
      end
      if reply = spec.use('reply')
        assert_matches(actual.reply, reply, 'Command reply does not match expectation')
      end
      if interrupt_in_use_connections = spec.use('interruptInUseConnections')
        assert_matches(actual.options[:interrupt_in_use_connections], interrupt_in_use_connections, 'Command interrupt_in_use_connections does not match expectation')
      end
      unless spec.empty?
        raise NotImplementedError, "Unhandled keys: #{spec}"
      end
    end

    def assert_eq(actual, expected, msg)
      unless expected == actual
        raise Error::ResultMismatch, "#{msg}: expected #{expected}, actual #{actual}"
      end
    end

    def assert_gte(actual, expected, msg)
      unless actual >= expected
        raise Error::ResultMismatch, "#{msg}: expected #{expected}, actual #{actual}"
      end
    end

    def assert_matches(actual, expected, msg)
      if actual.nil?
        if expected.is_a?(Hash) && expected.keys == ["$$unsetOrMatches"]
          return
        elsif !expected.nil?
          raise Error::ResultMismatch, "#{msg}: expected #{expected} but got nil"
        end
      end

      case expected
      when Array
        unless Array === actual
          raise Error::ResultMismatch, "Expected an array, found #{actual}"
        end
        unless actual.length == expected.length
          raise Error::ResultMismatch, "Expected array of length #{expected.length}, found array of length #{actual.length}: #{actual}"
        end
        expected.each_with_index do |v, i|
          assert_matches(actual[i], v, "#{msg}: index #{i}")
        end
      when Hash
        if expected.keys == %w($$unsetOrMatches) && expected.values.first.keys == %w(insertedId)
          actual_v = actual.inserted_id
          expected_v = expected.values.first.values.first
          assert_value_matches(actual_v, expected_v, 'inserted_id')
        elsif expected.keys == %w(insertedId)
          actual_v = actual.inserted_id
          expected_v = expected.values.first
          assert_value_matches(actual_v, expected_v, 'inserted_id')
        else
          if expected.empty?
            # This needs to be a match assertion. Check type only
            # and allow BulkWriteResult and generic operation result.
            unless Hash === actual || Mongo::BulkWrite::Result === actual || Mongo::Operation::Result === actual || Mongo::Crypt::RewrapManyDataKeyResult === actual
              raise Error::ResultMismatch, "#{msg}: expected #{expected}, actual #{actual}"
            end
          else
            expected.each do |k, expected_v|
              if k.start_with?('$$')
                assert_value_matches(actual, expected, k)
              else
                actual_v = actual[k]
                if Hash === expected_v && expected_v.length == 1 && expected_v.keys.first.start_with?('$$')
                  assert_value_matches(actual_v, expected_v, k)
                else
                  assert_matches(actual_v, expected_v, "#{msg}: key #{k}")
                end
              end
            end
          end
        end
      else
        if Integer === expected && BSON::Int64 === actual
          actual = actual.value
        end
        unless actual == expected
          raise Error::ResultMismatch, "#{msg}: expected #{expected}, actual #{actual}"
        end
      end
    end

    def assert_type(object, type)
      ok = [*type].reduce(false) { |acc, x| acc || type_matches?(object, x) }

      unless ok
        raise Error::ResultMismatch, "Object #{object} is not of type #{type}"
      end
    end

    def type_matches?(object, type)
      ok = case type
      when 'object'
        Hash === object
      when 'int', 'long'
        Integer === object || BSON::Int32 === object || BSON::Int64 === object
      when 'objectId'
        BSON::ObjectId === object
      when 'date'
        Time === object
      when 'double'
        Float === object
      when 'string'
        String === object
      when 'binData'
        BSON::Binary === object
      when 'array'
        Array === object
      else
        raise NotImplementedError, "Unhandled type #{type}"
      end
    end

    def assert_value_matches(actual, expected, msg)
      if Hash === expected && expected.keys.length == 1 &&
        (operator = expected.keys.first).start_with?('$$')
      then
        expected_v = expected.values.first
        case operator
        when '$$unsetOrMatches'
          if actual
            if Mongo::BulkWrite::Result === actual || Mongo::Operation::Result === actual
              assert_result_matches(actual, UsingHash[expected_v])
            else
              assert_matches(actual, expected_v, msg)
            end
          end
        when '$$matchesHexBytes'
          expected_data = decode_hex_bytes(expected_v)
          unless actual == expected_data
            raise Error::ResultMismatch, "Hex bytes do not match"
          end
        when '$$exists'
          case expected_v
          when true
            if actual.nil?
              raise Error::ResultMismatch, "#{msg}: wanted value to exist, but it did not"
            end
          when false
            if actual
              raise Error::ResultMismatch, "#{msg}: wanted value to not exist, but it did"
            end
          else
            raise NotImplementedError, "Bogus value #{expected_v}"
          end
        when '$$sessionLsid'
          expected_session = entities.get(:session, expected_v)
          # TODO - sessions do not expose server sessions after being ended
          #unless actual_v == {'id' => expected_session.server_session.session_id.to_bson}
          #  raise Error::ResultMismatch, "Session does not match: wanted #{expected_session}, have #{actual_v}"
          #end
        when '$$type'
          assert_type(actual, expected_v)
        when '$$matchesEntity'
          result = entities.get(:result, expected_v)
          unless actual == result
            raise Error::ResultMismatch, "Actual value #{actual} does not match entity #{expected_v} with value #{result}"
          end
        else
          raise NotImplementedError, "Unknown operator #{operator}"
        end
      else
        if actual != expected
          raise Error::ResultMismatch, "Mismatch for #{msg}: expected #{expected}, have #{actual}"
        end
      end
    end
  end
end

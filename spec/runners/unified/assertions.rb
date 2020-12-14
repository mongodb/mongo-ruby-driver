module Unified

  module Assertions

    def assert_result_matches(actual, expected)
      use_all(expected, 'expected result', expected) do |expected|
        %w(deleted inserted matched modified upserted).each do |k|
          if count = expected.use("#{k}Count")
            if Hash === count || count > 0
              actual_count = case actual
              when Mongo::BulkWrite::Result, Mongo::Operation::Delete::Result
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
                raise "Actual not empty"
              end
            else
              if actual_v != expected_v
                raise "Mismatch: actual #{actual_v}, expected #{expected_v}"
              end
            end
          end
        end

        assert_matches(actual, expected, 'result')
        expected.clear
      end
    end

    def assert_outcome
      return unless outcome

      client = ClientRegistry.instance.global_client('authorized')
      outcome.each do |spec|
        spec = UsingHash[spec]
        collection = client.use(spec.use!('databaseName'))[spec.use!('collectionName')]
        expected_docs = spec.use!('documents')
        actual_docs = collection.find({}, order: :_id).to_a
        assert_documents_match(actual_docs, expected_docs)
        unless spec.empty?
          raise "Unhandled keys: #{spec}"
        end
      end
    end

    def assert_documents_match(actual, expected)
      unless actual.length == expected.length
        raise "Unexpected number of documents: expected #{expected.length}, actual #{actual.length}"
      end

      actual.each_with_index do |document, index|
        assert_matches(document, expected[index], "document ##{index}")
      end
    end

    def assert_document_matches(actual, expected, msg)
      unless actual == expected
    p actual
    p expected
        raise "#{msg} does not match"
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
        actual_events = subscriber.wanted_events
        unless actual_events.length == expected_events.length
          raise "Event count mismatch: expected #{expected_events.length}, actual #{actual_events.length}\nExpected: #{expected_events}\nActual: #{actual_events}"
        end
        expected_events.each_with_index do |event, i|
          assert_event_matches(actual_events[i], event)
        end
      end
    end

    def assert_event_matches(actual, expected)
      assert_eq(expected.keys.length, 1, "Expected event must have one key: #{expected}")
      expected_name, spec = expected.first
      spec = UsingHash[spec]
      expected_name = expected_name.sub(/Event$/, '').sub(/^(.)/) { $1.upcase }
      assert_eq(actual.class.name.sub(/.*::/, ''), expected_name, 'Event name does not match')
      if db_name = spec.use('databaseName')
        assert_eq(actual.database_name, db_name, 'Database names differ')
      end
      if command_name = spec.use('commandName')
        assert_eq(actual.command_name, command_name, 'Command names differ')
      end
      if command = spec.use('command')
        assert_matches(actual.command, command, 'Commands differ')
      end
    end

    def assert_eq(actual, expected, msg)
      unless expected == actual
        raise "#{msg}: expected #{expected}, actual #{actual}"
      end
    end

    def assert_matches(actual, expected, msg)
      if actual.nil? && !expected.nil?
        raise "#{msg}: expected #{expected} but got nil"
      end

      case expected
      when Array
        unless Array === actual
          raise "Expected an array, found #{actual}"
        end
        unless actual.length == expected.length
          raise "Expected array of length #{expected.length}, found array of length #{actual.length}: #{actual}"
        end
        expected.each_with_index do |v, i|
          assert_matches(actual[i], v, "#{msg}: index #{i}")
        end
      when Hash
        if expected.keys == %w($$unsetOrMatches) && expected.values.first.keys == %w(insertedId)
          actual_v = actual.inserted_id
          expected_v = expected.values.first.values.first
          assert_value_matches(actual_v, expected_v, 'inserted_id')
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
      else
        if Integer === expected && BSON::Int64 === actual
          actual = actual.value
        end
        unless actual == expected
          raise "#{msg}: expected #{expected}, actual #{actual}"
        end
      end
    end

    def assert_type(object, type)
      ok = case type
      when 'object'
        Hash === object
      when %w(int long)
        Integer === object || BSON::Int32 === object || BSON::Int64 === object
      when 'objectId'
        BSON::ObjectId === object
      when 'date'
        Time === object
      else
        raise "Unhandled type #{type}"
      end
      unless ok
        raise "Object #{object} is not of type #{type}"
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
            unless actual == expected_v
              raise "Mismatch for #{msg}: expected #{expected}, have #{actual}"
            end
          end
        when '$$matchesHexBytes'
          expected_data = decode_hex_bytes(expected_v)
          unless actual == expected_data
            raise "Hex bytes do not match"
          end
        when '$$exists'
          case expected_v
          when true
            if actual.nil?
              raise "#{msg}: wanted value to exist, but it did not"
            end
          when false
            if actual
              raise "#{msg}: wanted value to not exist, but it did"
            end
          else
            raise "Bogus value #{expected_v}"
          end
        when '$$sessionLsid'
          expected_session = entities.get(:session, expected_v)
          # TODO - sessions do not expose server sessions after being ended
          #unless actual_v == {'id' => expected_session.server_session.session_id.to_bson}
          #  raise "Session does not match: wanted #{expected_session}, have #{actual_v}"
          #end
        when '$$type'
          assert_type(actual, expected_v)
        when '$$matchesEntity'
          result = entities.get(:result, expected_v)
          unless actual == result
            raise "Actual value #{actual} does not match entity #{expected_v} with value #{result}"
          end
        else
          raise "Unknown operator #{operator}"
        end
      else
        if actual != expected
          raise "Mismatch for #{msg}: expected #{expected}, have #{actual}"
        end
      end
    end
  end
end

require 'runners/crud/requirement'
require 'runners/unified/crud_operations'
require 'runners/unified/grid_fs_operations'
require 'runners/unified/ddl_operations'
require 'runners/unified/change_stream_operations'
require 'runners/unified/support_operations'
require 'runners/unified/assertions'

module Unified

  class Test
    include CrudOperations
    include GridFsOperations
    include DdlOperations
    include ChangeStreamOperations
    include SupportOperations
    include Assertions

    def initialize(spec)
      @spec = spec
      @entities = EntityMap.new
      @test_spec = UsingHash[@spec.fetch('test')]
      @description = @test_spec.use('description')
      @outcome = @test_spec.use('outcome')
      @expected_events = @test_spec.use('expectEvents')
      @skip_reason = @test_spec.use('skipReason')
      if req = @test_spec.use('runOnRequirements')
        @reqs = req.map { |r| Mongo::CRUD::Requirement.new(r) }
      end
      if req = @spec['group_runOnRequirements']
        @group_reqs = req.map { |r| Mongo::CRUD::Requirement.new(r) }
      end
      mongoses = @spec['createEntities'].select do |spec|
        spec['client']
      end.map do |spec|
        spec['client']['useMultipleMongoses']
      end.compact.uniq
      if mongoses.length > 1
        raise "Conflicting useMultipleMongoses values"
      end
      @multiple_mongoses = mongoses.first
      @test_spec.freeze
      @subscribers = {}
    end

    attr_reader :test_spec
    attr_reader :description
    attr_reader :outcome
    attr_reader :skip_reason
    attr_reader :reqs, :group_reqs

    def skip?
      !!@skip_reason
    end

    def require_multiple_mongoses?
      @multiple_mongoses == true
    end

    def require_single_mongos?
      @multiple_mongoses == false
    end

    attr_reader :entities

    def create_entities
      @spec['createEntities'].each do |entity_spec|
        unless entity_spec.keys.length == 1
          raise "Entity must have exactly one key"
        end

        type, spec = entity_spec.first
        spec = UsingHash[spec]
        id = spec.use!('id')

        entity = case type
        when 'client'
          # Handled earlier
          spec.delete('useMultipleMongoses')

          if smc_opts = spec.use('uriOptions')
            opts = Mongo::URI::OptionsMapper.new.smc_to_ruby(smc_opts)
          else
            opts = {}
          end

          Mongo::Client.new(
            SpecConfig.instance.addresses,
            SpecConfig.instance.all_test_options.update(
              max_read_retries: 0,
              max_write_retries: 0,
            ).update(opts),
          ).tap do |client|
            if oe = spec.use('observeEvents')
              oe.each do |event|
                case event
                when 'commandStartedEvent', 'commandSucceededEvent', 'commandFailedEvent'
                  subscriber = (@subscribers[client] ||= EventSubscriber.new)
                  unless client.send(:monitoring).subscribers[Mongo::Monitoring::COMMAND].include?(subscriber)
                    client.subscribe(Mongo::Monitoring::COMMAND, subscriber)
                  end
                  kind = event.sub('command', '').sub('Event', '').downcase.to_sym
                  subscriber.add_wanted_events(kind)
                  if ignore_events = spec.use('ignoreCommandMonitoringEvents')
                    subscriber.ignore_commands(ignore_events)
                  end
                else
                  raise "Unknown event #{event}"
                end
              end
            end
          end
        when 'database'
          client = entities.get(:client, spec.use!('client'))
          client.use(spec.use!('databaseName')).database
        when 'collection'
          database = entities.get(:database, spec.use!('database'))
          # TODO verify
          opts = Utils.snakeize_hash(spec.use('collectionOptions') || {})
          database[spec.use!('collectionName'), opts]
        when 'bucket'
          database = entities.get(:database, spec.use!('database'))
          database.fs
        when 'session'
          client = entities.get(:client, spec.use!('client'))

          if smc_opts = spec.use('sessionOptions')
            opts = ::Utils.underscore_hash(smc_opts)
          else
            opts = {}
          end

          client.start_session(**opts)
        else
          raise "Unknown type #{type}"
        end
        unless spec.empty?
          raise "Unhandled spec keys: #{spec}"
        end
        entities.set(type.to_sym, id, entity)
      end
    end

    def set_initial_data
      client = ClientRegistry.instance.global_client('authorized')

      @spec['initialData'].each do |entity_spec|
        spec = UsingHash[entity_spec]
        collection = client.use(spec.use!('databaseName'))[spec.use!('collectionName')]
        collection.delete_many
        docs = spec.use!('documents')
        if docs.any?
          collection.insert_many(docs)
        else
          begin
            collection.create
          rescue Mongo::Error => e
            if e.code == 48
              # Already exists
            else
              raise
            end
          end
        end
        unless spec.empty?
          raise "Unhandled spec keys: #{spec}"
        end
      end
    end

    def run
      test_spec = UsingHash[self.test_spec]
      ops = test_spec.use!('operations')
      execute_operations(ops)
      unless test_spec.empty?
        raise "Unhandled spec keys: #{test_spec}"
      end
    ensure
      disable_fail_points
    end

    def execute_operations(ops)
      ops.each do |op|
        execute_operation(op)
      end
    end

    def execute_operation(op)
      use_all(op, 'operation', op) do |op|
        name = Utils.underscore(op.use!('name'))
        if expected_error = op.use('expectError')
          begin
            send(name, op)
          rescue Mongo::Error, BSON::String::IllegalKey => e
            if expected_error.use('isClientError')
              unless BSON::String::IllegalKey === e
                raise "Expected client error but got #{e}"
              end
            end
            if code_name = expected_error.use('errorCodeName')
              unless e.code_name == code_name
                raise "Expected #{code_name} code but had #{e.code_name}"
              end
            end
            if labels = expected_error.use('errorLabelsContain')
              labels.each do |label|
                unless e.label?(label)
                  raise "Expected error to contain label #{label} but it did not"
                end
              end
            end
            if omit_labels = expected_error.use('errorLabelsOmit')
              omit_labels.each do |label|
                if e.label?(label)
                  raise "Expected error to not contain label #{label} but it did"
                end
              end
            end
            if expected_result = expected_error.use('expectResult')
              assert_result_matches(e.result, expected_result)
              #expected_result.clear
            # Important: this must be the last branch.
            elsif expected_error.use('isError')
              # Nothing but we consume the key.
            end
            unless expected_error.empty?
              raise "Unhandled keys: #{expected_error}"
            end
          else
            raise "Expected exception but none was raised"
          end
        else
          result = send(name, op)
          if expected_result = op.use('expectResult')
            if !expected_result.empty? && result.nil?
              raise "Actual result nil but expected result #{expected_result}"
            elsif Array === expected_result
              assert_documents_match(result, expected_result)
            else
              assert_result_matches(result, expected_result)
            end
            #expected_result.clear
          end
          if save_entity = op.use('saveResultAsEntity')
            entities.set(:result, save_entity, result)
          end
        end
      end
    end

    def use_sub(hash, key, &block)
      v = hash.use!(key)
      use_all(hash, key, v, &block)
    end

    def use_all(hash, key, v)
      orig_v = v.dup
      (yield v).tap do
        unless v.empty?
          raise "Unconsumed items for #{key}: #{v}\nOriginal hash: #{orig_v}"
        end
      end
    end

    def use_arguments(op, &block)
      use_sub(op, 'arguments', &block)
    end

    def cleanup
      if $kill_transactions || true
        ClientRegistry.instance.global_client('authorized').command(
          killAllSessions: [],
        ) rescue nil
        $kill_transactions = nil
      end

      entities[:client]&.each do |id, client|
        client.close
      end
    end

    def disable_fail_points
      if $disable_fail_points
        $disable_fail_points.each do |(fail_point_command, address)|
          client = ClusterTools.instance.direct_client(address,
            database: 'admin')
          client.command(configureFailPoint: fail_point_command['configureFailPoint'],
            mode: 'off')
        end
        $disable_fail_points = nil
      end
    end
  end
end

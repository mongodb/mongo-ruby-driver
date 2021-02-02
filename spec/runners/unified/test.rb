require 'runners/crud/requirement'
require 'runners/unified/crud_operations'
require 'runners/unified/grid_fs_operations'
require 'runners/unified/ddl_operations'
require 'runners/unified/change_stream_operations'
require 'runners/unified/support_operations'
require 'runners/unified/assertions'
require 'support/utils'

module Unified

  class Test
    include CrudOperations
    include GridFsOperations
    include DdlOperations
    include ChangeStreamOperations
    include SupportOperations
    include Assertions

    def initialize(spec, **opts)
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
        raise Error::InvalidTest, "Conflicting useMultipleMongoses values"
      end
      @multiple_mongoses = mongoses.first
      @test_spec.freeze
      @subscribers = {}
      @options = opts
    end

    attr_reader :test_spec
    attr_reader :description
    attr_reader :outcome
    attr_reader :skip_reason
    attr_reader :reqs, :group_reqs
    attr_reader :options

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
          raise NotImplementedError, "Entity must have exactly one key"
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

          if store_events = spec.use('storeEventsAsEntities')
            store_event_names = {}
            store_events.each do |event_name, entity_name|
              #event_name = event_name.gsub(/Event$/, '').gsub(/[A-Z]/) { |m| "_#{m}" }.upcase
              #event_name = event_name.gsub(/Event$/, '').sub(/./) { |m| m.upcase }
              store_event_names[event_name] = entity_name
            end
            store_event_names.values.uniq.each do |entity_name|
              entities.set(:event_list, entity_name, [])
            end
            subscriber = StoringEventSubscriber.new do |payload|
              if entity_name = store_event_names[payload['name']]
                entities.get(:event_list, entity_name) << payload
              end
            end
            opts[:sdam_proc] = lambda do |client|
              client.subscribe(Mongo::Monitoring::COMMAND, subscriber)
              client.subscribe(Mongo::Monitoring::CONNECTION_POOL, subscriber)
            end
          end

          create_client(**opts).tap do |client|
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
                  raise NotImplementedError, "Unknown event #{event}"
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
          raise NotImplementedError, "Unknown type #{type}"
        end
        unless spec.empty?
          raise NotImplementedError, "Unhandled spec keys: #{spec}"
        end
        entities.set(type.to_sym, id, entity)
      end
    end

    def set_initial_data
      @spec['initialData']&.each do |entity_spec|
        spec = UsingHash[entity_spec]
        collection = root_authorized_client.use(spec.use!('databaseName'))[spec.use!('collectionName')]
        collection.drop
        docs = spec.use!('documents')
        if docs.any?
          collection.insert_many(docs)
        else
          begin
            collection.create
          rescue Mongo::Error => e
            if Mongo::Error::OperationFailure === e && (
              e.code == 48 || e.message =~ /collection already exists/
            )
              # Already exists
            else
              raise
            end
          end
        end
        unless spec.empty?
          raise NotImplementedError, "Unhandled spec keys: #{spec}"
        end
      end
    end

    def run
      test_spec = UsingHash[self.test_spec]
      ops = test_spec.use!('operations')
      execute_operations(ops)
      unless test_spec.empty?
        raise NotImplementedError, "Unhandled spec keys: #{test_spec}"
      end
    ensure
      disable_fail_points
    end

    def stop!
      @stop = true
    end

    def stop?
      !!@stop
    end

    def cleanup
      if $kill_transactions || true
        begin
          root_authorized_client.command(
            killAllSessions: [],
          )
        rescue Mongo::Error::OperationFailure => e
          if e.code == 11601
            # operation was interrupted, ignore
          elsif e.code == 59
            # no such command (old server), ignore
          else
            raise
          end
        end
        $kill_transactions = nil
      end

      entities[:client]&.each do |id, client|
        client.close
      end
    end

    private

    def execute_operations(ops)
      ops.each do |op|
        execute_operation(op)
      end
    end

    def execute_operation(op)
      use_all(op, 'operation', op) do |op|
        name = Utils.underscore(op.use!('name'))
        method_name = name
        if name.to_s == 'loop'
          method_name = "_#{name}"
        end
        if expected_error = op.use('expectError')
          begin
            send(method_name, op)
          rescue Mongo::Error, BSON::String::IllegalKey => e
            if expected_error.use('isClientError')
              unless BSON::String::IllegalKey === e
                raise Error::ErrorMismatch, "Expected client error but got #{e}"
              end
            end
            if code_name = expected_error.use('errorCodeName')
              unless e.code_name == code_name
                raise Error::ErrorMismatch, "Expected #{code_name} code but had #{e.code_name}"
              end
            end
            if labels = expected_error.use('errorLabelsContain')
              labels.each do |label|
                unless e.label?(label)
                  raise Error::ErrorMismatch, "Expected error to contain label #{label} but it did not"
                end
              end
            end
            if omit_labels = expected_error.use('errorLabelsOmit')
              omit_labels.each do |label|
                if e.label?(label)
                  raise Error::ErrorMismatch, "Expected error to not contain label #{label} but it did"
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
              raise NotImplementedError, "Unhandled keys: #{expected_error}"
            end
          else
            raise Error::ErrorMismatch, "Expected exception but none was raised"
          end
        else
          result = send(method_name, op)
          if expected_result = op.use('expectResult')
            if !expected_result.empty? && result.nil?
              raise Error::ResultMismatch, "Actual result nil but expected result #{expected_result}"
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
          raise NotImplementedError, "Unconsumed items for #{key}: #{v}\nOriginal hash: #{orig_v}"
        end
      end
    end

    def use_arguments(op, &block)
      use_sub(op, 'arguments', &block)
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

    def root_authorized_client
      @root_authorized_client ||= ClientRegistry.instance.global_client('root_authorized')
    end

    def create_client(**opts)
      args = case v = options[:client_args]
      when Array
        unless v.length == 2
          raise NotImplementedError, 'Client args array must have two elements'
        end
        [v.first, v.last.dup]
      when String
        [v, {}]
      else
        [
          SpecConfig.instance.addresses,
          SpecConfig.instance.all_test_options,
        ]
      end
      args.last.update(
        max_read_retries: 0,
        max_write_retries: 0,
      ).update(opts)
      Mongo::Client.new(*args)
    end
  end
end

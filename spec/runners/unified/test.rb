# frozen_string_literal: true
# rubocop:todo all

require 'runners/crud/requirement'
require 'runners/unified/client_side_encryption_operations'
require 'runners/unified/crud_operations'
require 'runners/unified/grid_fs_operations'
require 'runners/unified/ddl_operations'
require 'runners/unified/change_stream_operations'
require 'runners/unified/support_operations'
require 'runners/unified/thread_operations'
require 'runners/unified/search_index_operations'
require 'runners/unified/assertions'
require 'support/utils'
require 'support/crypt'

module Unified

  class Test
    include ClientSideEncryptionOperations
    include CrudOperations
    include GridFsOperations
    include DdlOperations
    include ChangeStreamOperations
    include SupportOperations
    include ThreadOperations
    include SearchIndexOperations
    include Assertions
    include RSpec::Core::Pending

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
      @multiple_mongoses = mongoses.any? { |v| v }
      @test_spec.freeze
      @subscribers = {}
      @observe_sensitive = {}
      @options = opts
    end

    attr_reader :test_spec
    attr_reader :description
    attr_reader :outcome
    attr_reader :skip_reason
    attr_reader :reqs, :group_reqs
    attr_reader :options

    def retry?
      @description =~ /KMS/i
    end

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

    def create_spec_entities
      return if @entities_created
      generate_entities(@spec['createEntities'])
    end

    def generate_entities(es)
      es.each do |entity_spec|
        unless entity_spec.keys.length == 1
          raise NotImplementedError, "Entity must have exactly one key"
        end

        type, spec = entity_spec.first
        spec = UsingHash[spec]
        id = spec.use!('id')

        entity = case type
        when 'client'
          if smc_opts = spec.use('uriOptions')
            opts = Mongo::URI::OptionsMapper.new.smc_to_ruby(smc_opts)
          else
            opts = {}
          end

          # max_pool_size gets automatically set to 3 if not explicitly set by
          # the test, therefore, if min_pool_size is set, make sure to set the
          # max_pool_size as well to something greater.
          if !opts.key?('max_pool_size') && min_pool_size = opts[:min_pool_size]
            opts[:max_pool_size] = min_pool_size + 3
          end

          if spec.use('useMultipleMongoses')
            if ClusterConfig.instance.topology == :sharded
              unless SpecConfig.instance.addresses.length > 1
                raise "useMultipleMongoses requires more than one address in MONGODB_URI"
              end
            end
          else
            # If useMultipleMongoses isn't true, truncate the address
            # list to the first address.
            # This works OK in replica sets because the driver will discover
            # the other set members, in standalone deployments because
            # there is only one server, but changes behavior in
            # sharded clusters compared to how the test suite is configured.
            options[:single_address] = true
          end

          if store_events = spec.use('storeEventsAsEntities')
            store_event_names = {}
            store_events.each do |spec|
              entity_name = spec['id']
              event_names = spec['events']
              event_names.each do |event_name|
                store_event_names[event_name] = entity_name
              end
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

          if server_api = spec.use('serverApi')
            server_api = ::Utils.underscore_hash(server_api)
            opts[:server_api] = server_api
          end

          observe_events = spec.use('observeEvents')
          subscriber = EventSubscriber.new
          current_proc = opts[:sdam_proc]
          opts[:sdam_proc] = lambda do |client|
            current_proc.call(client) if current_proc
            if oe = observe_events
              oe.each do |event|
                case event
                when 'commandStartedEvent', 'commandSucceededEvent', 'commandFailedEvent'
                  unless client.send(:monitoring).subscribers[Mongo::Monitoring::COMMAND].include?(subscriber)
                    client.subscribe(Mongo::Monitoring::COMMAND, subscriber)
                  end
                  kind = event.sub('command', '').sub('Event', '').downcase.to_sym
                  subscriber.add_wanted_events(kind)
                  if ignore_events = spec.use('ignoreCommandMonitoringEvents')
                    subscriber.ignore_commands(ignore_events)
                  end
                when /\A(?:pool|connection)/
                  unless client.send(:monitoring).subscribers[Mongo::Monitoring::CONNECTION_POOL]&.include?(subscriber)
                    client.subscribe(Mongo::Monitoring::CONNECTION_POOL, subscriber)
                  end
                  kind = event.sub('Event', '').gsub(/([A-Z])/) { "_#{$1}" }.sub('pool', 'Pool').downcase.to_sym
                  subscriber.add_wanted_events(kind)
                when 'serverDescriptionChangedEvent'
                  unless client.send(:monitoring).subscribers[Mongo::Monitoring::SERVER_DESCRIPTION_CHANGED]&.include?(subscriber)
                    client.subscribe(Mongo::Monitoring::SERVER_DESCRIPTION_CHANGED, subscriber)
                  end
                  kind = event.sub('Event', '').gsub(/([A-Z])/) { "_#{$1}" }.downcase.to_sym
                  subscriber.add_wanted_events(kind)
                else
                  raise NotImplementedError, "Unknown event #{event}"
                end
              end
            end
          end

          create_client(**opts).tap do |client|
            @observe_sensitive[id] = spec.use('observeSensitiveCommands')
            @subscribers[client] ||= subscriber
          end
        when 'database'
          client = entities.get(:client, spec.use!('client'))
          opts = Utils.snakeize_hash(spec.use('databaseOptions') || {})
            .merge(database: spec.use!('databaseName'))
          if opts.key?(:read_preference)
            opts[:read] = opts.delete(:read_preference)
            if opts[:read].key?(:max_staleness_seconds)
              opts[:read][:max_staleness] = opts[:read].delete(:max_staleness_seconds)
            end
          end
          client.with(opts).database
        when 'collection'
          database = entities.get(:database, spec.use!('database'))
          # TODO verify
          opts = Utils.snakeize_hash(spec.use('collectionOptions') || {})
          if opts.key?(:read_preference)
            opts[:read] = opts.delete(:read_preference)
            if opts[:read].key?(:max_staleness_seconds)
              opts[:read][:max_staleness] = opts[:read].delete(:max_staleness_seconds)
            end
          end
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
        when 'clientEncryption'
          client_encryption_opts = spec.use!('clientEncryptionOpts')
          key_vault_client = entities.get(:client, client_encryption_opts['keyVaultClient'])
          opts = {
            key_vault_namespace: client_encryption_opts['keyVaultNamespace'],
            kms_providers: Utils.snakeize_hash(client_encryption_opts['kmsProviders']),
            kms_tls_options: {
              kmip: {
                ssl_cert: SpecConfig.instance.fle_kmip_tls_certificate_key_file,
                ssl_key: SpecConfig.instance.fle_kmip_tls_certificate_key_file,
                ssl_ca_cert: SpecConfig.instance.fle_kmip_tls_ca_file
              }
            }
          }
          opts[:kms_providers] = opts[:kms_providers].map do |provider, options|
            converted_options = options.map do |key, value|
              converted_value = if value == { '$$placeholder'.to_sym => 1 }
                case provider
                when :aws
                  case key
                  when :access_key_id then SpecConfig.instance.fle_aws_key
                  when :secret_access_key then SpecConfig.instance.fle_aws_secret
                  end
                when :azure
                  case key
                  when :tenant_id then SpecConfig.instance.fle_azure_tenant_id
                  when :client_id then SpecConfig.instance.fle_azure_client_id
                  when :client_secret then SpecConfig.instance.fle_azure_client_secret
                  end
                when :gcp
                  case key
                  when :email then SpecConfig.instance.fle_gcp_email
                  when :private_key then SpecConfig.instance.fle_gcp_private_key
                  end
                when :kmip
                  case key
                  when :endpoint then SpecConfig.instance.fle_kmip_endpoint
                  end
                when :local
                  case key
                  when :key then Crypt::LOCAL_MASTER_KEY
                  end
                end
              else
                value
              end
              [key, converted_value]
            end.to_h
            [provider, converted_options]
          end.to_h

          Mongo::ClientEncryption.new(
            key_vault_client,
            opts
          )
        when 'thread'
          thread_context = ThreadContext.new
          thread = Thread.new do
            loop do
              begin
                op_spec = thread_context.operations.pop(true)
                execute_operation(op_spec)
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
          thread
        else
          raise NotImplementedError, "Unknown type #{type}"
        end
        unless spec.empty?
          raise NotImplementedError, "Unhandled spec keys: #{spec}"
        end
        entities.set(type.to_sym, id, entity)
      end
      @entities_created = true
    end

    def set_initial_data
      @spec['initialData']&.each do |entity_spec|
        spec = UsingHash[entity_spec]
        collection = root_authorized_client.with(write_concern: {w: :majority}).
          use(spec.use!('databaseName'))[spec.use!('collectionName')]
        collection.drop
        create_options = spec.use('createOptions') || {}
        docs = spec.use!('documents')
        begin
          collection.create(create_options)
        rescue Mongo::Error => e
          if Mongo::Error::OperationFailure === e && (
              e.code == 48 || e.message =~ /collection already exists/
          )
            # Already exists
          else
            raise
          end
        end
        if docs.any?
          collection.insert_many(docs)
        end
        unless spec.empty?
          raise NotImplementedError, "Unhandled spec keys: #{spec}"
        end
      end
    end

    def run
      kill_sessions

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
        kill_sessions
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

        if ["modify_collection", "list_index_names"].include?(name.to_s)
          skip "Mongo Ruby Driver does not support #{name.to_s}"
        end

        if expected_error = op.use('expectError')
          begin
            unless respond_to?(method_name)
              raise Error::UnsupportedOperation, "Mongo Ruby Driver does not support #{name.to_s}"
            end

            public_send(method_name, op)
          rescue Mongo::Error, bson_error, Mongo::Auth::Unauthorized, ArgumentError => e
            if expected_error.use('isClientError')
              # isClientError doesn't actually mean a client error.
              # It means anything other than OperationFailure. DRIVERS-1799
              if Mongo::Error::OperationFailure === e
                raise Error::ErrorMismatch, %Q,Expected not OperationFailure ("isClientError") but got #{e},
              end
            end
            if code = expected_error.use('errorCode')
              unless e.code == code
                raise Error::ErrorMismatch, "Expected #{code} code but had #{e.code}"
              end
            end
            if code_name = expected_error.use('errorCodeName')
              unless e.code_name == code_name
                raise Error::ErrorMismatch, "Expected #{code_name} code name but had #{e.code_name}"
              end
            end
            if text = expected_error.use('errorContains')
              unless e.to_s.include?(text)
                raise Error::ErrorMismatch, "Expected #{text} in the message but had #{e}"
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
            if error_response = expected_error.use("errorResponse")
              assert_result_matches(e.document, error_response)
            end
            if expected_result = expected_error.use('expectResult')
              assert_result_matches(e.result, expected_result)
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
        elsif op.use('ignoreResultAndError')
          unless respond_to?(method_name)
            raise Error::UnsupportedOperation, "Mongo Ruby Driver does not support #{name.to_s}"
          end

          begin
            send(method_name, op)
          # We can possibly rescue more errors here, add as needed.
          rescue Mongo::Error
          end
        else
          unless respond_to?(method_name, true)
            raise Error::UnsupportedOperation, "Mongo Ruby Driver does not support #{name.to_s}"
          end

          result = send(method_name, op)
          if expected_result = op.use('expectResult')
            if result.nil? && expected_result.keys == ["$$unsetOrMatches"]
              return
            elsif result.nil? && !expected_result.empty?
              raise Error::ResultMismatch, "#{msg}: expected #{expected} but got nil"
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
      if op.key?('arguments')
        use_sub(op, 'arguments', &block)
      else
        yield UsingHash.new
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

    def kill_sessions
      begin
        root_authorized_client.command(
          killAllSessions: [],
        )
      rescue Mongo::Error::OperationFailure => e
        if e.code == 11601
          # operation was interrupted, ignore. SERVER-38335
        elsif e.code == 13
          # Unauthorized - e.g. when running in Atlas as part of
          # drivers-atlas-testing, ignore. SERVER-54216
        elsif e.code == 59
          # no such command (old server), ignore
        elsif e.code == 8000
          # CMD_NOT_ALLOWED: killAllSessions - running against a serverless instance
        else
          raise
        end
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
        addresses = SpecConfig.instance.addresses
        if options[:single_address]
          addresses = [addresses.first]
        end
        [
          addresses,
          SpecConfig.instance.all_test_options,
        ]
      end
      args.last.update(
        max_read_retries: 0,
        max_write_retries: 0,
      ).update(opts)
      Mongo::Client.new(*args)
    end

    # The error to rescue BSON tests for. If we still define
    # BSON::String::IllegalKey then we should rescue that particular error,
    # otherwise, rescue an arbitrary BSON::Error
    def bson_error
      BSON::String.const_defined?(:IllegalKey) ?
        BSON::String.const_get(:IllegalKey) :
        BSON::Error
    end
  end
end

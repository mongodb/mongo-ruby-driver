# frozen_string_literal: true
# rubocop:todo all

module Unified

  module DdlOperations

    def list_databases(op)
      list_dbs(op, name_only: false)
    end

    def list_database_names(op)
      list_dbs(op, name_only: false)
    end

    def list_dbs(op, name_only: false)
      client = entities.get(:client, op.use!('object'))
      use_arguments(op) do |args|
        opts = {}
        if session = args.use('session')
          opts[:session] = entities.get(:session, session)
        end
        client.list_databases(args.use('filter') || {}, name_only, **opts)
      end
    end

    def create_collection(op)
      database = entities.get(:database, op.use!('object'))
      use_arguments(op) do |args|
        opts = {}
        if session = args.use('session')
          opts[:session] = entities.get(:session, session)
        end
        collection_opts = {}
        if timeseries = args.use('timeseries')
          collection_opts[:time_series] = timeseries
        end
        if expire_after_seconds = args.use('expireAfterSeconds')
          collection_opts[:expire_after] = expire_after_seconds
        end
        if clustered_index = args.use('clusteredIndex')
          collection_opts[:clustered_index] = clustered_index
        end
        if change_stream_pre_and_post_images = args.use('changeStreamPreAndPostImages')
          collection_opts[:change_stream_pre_and_post_images] = change_stream_pre_and_post_images
        end
        if view_on = args.use('viewOn')
          collection_opts[:view_on] = view_on
        end
        if pipeline = args.use('pipeline')
          collection_opts[:pipeline] = pipeline
        end
        database[args.use!('collection'), collection_opts].create(**opts)
      end
    end

    def list_collections(op)
      list_colls(op, name_only: false)
    end

    def list_collection_names(op)
      list_colls(op, name_only: true)
    end

    def list_colls(op, name_only: false)
      database = entities.get(:database, op.use!('object'))
      use_arguments(op) do |args|
        opts = {}
        if session = args.use('session')
          opts[:session] = entities.get(:session, session)
        end
        if filter = args.use('filter')
          opts[:filter] = filter
        end
        database.list_collections(**opts.merge(name_only: name_only))
      end
    end

    def drop_collection(op)
      database = entities.get(:database, op.use!('object'))
      use_arguments(op) do |args|
        collection = database[args.use!('collection')]
        collection.drop
      end
    end

    def rename(op)
      collection = entities.get(:collection, op.use!('object'))
      use_arguments(op) do |args|
        to = args.use!('to')
        cmd = {
          renameCollection: "#{collection.database.name}.#{collection.name}",
          to: "#{collection.database.name}.#{to}"
        }

        if args.key?("dropTarget")
          cmd[:dropTarget] = args.use("dropTarget")
        end

        collection.client.use(:admin).command(**cmd)
      end
    end

    def assert_collection_exists(op, state = true)
      consume_test_runner(op)
      use_arguments(op) do |args|
        client = ClientRegistry.instance.global_client('authorized')
        database = client.use(args.use!('databaseName')).database
        collection_name = args.use!('collectionName')
        if state
          unless database.collection_names.include?(collection_name)
            raise Error::ResultMismatch, "Expected collection #{collection_name} to exist, but it does not"
          end
        else
          if database.collection_names.include?(collection_name)
            raise Error::ResultMismatch, "Expected collection #{collection_name} to not exist, but it does"
          end
        end
      end
    end

    def assert_collection_not_exists(op)
      assert_collection_exists(op, false)
    end

    def list_indexes(op)
      collection = entities.get(:collection, op.use!('object'))
      use_arguments(op) do |args|
        opts = {}
        if session = args.use('session')
          opts[:session] = entities.get(:session, session)
        end
        collection.indexes(**opts).to_a
      end
    end

    def create_index(op)
      collection = entities.get(:collection, op.use!('object'))
      use_arguments(op) do |args|
        opts = {}
        if session = args.use('session')
          opts[:session] = entities.get(:session, session)
        end
        if args.key?('unique')
          opts[:unique] = args.use('unique')
        end

        collection.indexes.create_one(
          args.use!('keys'),
          name: args.use('name'),
          **opts,
        )
      end
    end

    def drop_index(op)
      collection = entities.get(:collection, op.use!('object'))
      use_arguments(op) do |args|
        opts = {}
        if session = args.use('session')
          opts[:session] = entities.get(:session, session)
        end

        collection.indexes.drop_one(
          args.use!('name'),
          **opts,
        )
      end
    end


    def assert_index_exists(op)
      consume_test_runner(op)
      use_arguments(op) do |args|
        client = ClientRegistry.instance.global_client('authorized')
        database = client.use(args.use!('databaseName'))
        collection = database[args.use!('collectionName')]
        index = collection.indexes.get(args.use!('indexName'))
      end
    end

    def assert_index_not_exists(op)
      consume_test_runner(op)
      use_arguments(op) do |args|
        client = ClientRegistry.instance.global_client('authorized')
        database = client.use(args.use!('databaseName'))
        collection = database[args.use!('collectionName')]
        begin
          index = collection.indexes.get(args.use!('indexName'))
          raise Error::ResultMismatch, "Index found"
        rescue Mongo::Error::OperationFailure => e
          if e.code == 26
            # OK
          else
            raise
          end
        end
      end
    end

    def create_entities(op)
      consume_test_runner(op)
      use_arguments(op) do |args|
        generate_entities(args.use!('entities'))
      end
    end

    def record_topology_description(op)
      consume_test_runner(op)
      use_arguments(op) do |args|
        client = entities.get(:client, args.use!('client'))
        entities.set(:topology, args.use!('id'), client.cluster.topology)
      end
    end

    def assert_topology_type(op)
      consume_test_runner(op)
      use_arguments(op) do |args|
        topology = entities.get(:topology, args.use!('topologyDescription'))
        type = args.use!('topologyType')
        unless topology.display_name == type
          raise Error::ResultMismatch, "Expected topology type to be #{type}, but got #{topology.class}"
        end
      end
    end

    def retrieve_primary(topology)
      topology.server_descriptions.detect { |k, desc| desc.primary? }&.first
    end

    def wait_for_primary_change(op)
      consume_test_runner(op)
      use_arguments(op) do |args|
        client = entities.get(:client, args.use!('client'))
        topology = entities.get(:topology, args.use!('priorTopologyDescription'))
        timeout_ms = args.use('timeoutMS') || 10000
        old_primary = retrieve_primary(topology)

        deadline = Mongo::Utils.monotonic_time + timeout_ms / 1000.0
        loop do
          client.cluster.scan!
          new_primary = client.cluster.next_primary.address
          if new_primary && old_primary != new_primary
            break
          end
          if Mongo::Utils.monotonic_time >= deadline
            raise "Did not receive a change in primary from #{old_primary} in 10 seconds"
          else
            sleep 0.1
          end
        end
      end
    end
  end
end

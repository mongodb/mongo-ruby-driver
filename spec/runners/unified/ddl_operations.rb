# frozen_string_literal: true
# encoding: utf-8

module Unified

  module DdlOperations

    def list_databases(op)
      client = entities.get(:client, op.use!('object'))
      client.list_databases
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
        database[args.use!('collection'), collection_opts].create(**opts)
      end
    end

    def drop_collection(op)
      database = entities.get(:database, op.use!('object'))
      use_arguments(op) do |args|
        collection = database[args.use!('collection')]
        collection.drop
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

    def create_index(op)
      collection = entities.get(:collection, op.use!('object'))
      use_arguments(op) do |args|
        opts = {}
        if session = args.use('session')
          opts[:session] = entities.get(:session, session)
        end

        collection.indexes.create_one(
          args.use!('keys'),
          name: args.use!('name'),
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
  end
end

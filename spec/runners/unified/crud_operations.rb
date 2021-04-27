# frozen_string_literal: true
# encoding: utf-8

module Unified

  module CrudOperations

    def find(op)
      collection = entities.get(:collection, op.use!('object'))
      use_arguments(op) do |args|
        opts = {}
        if session = args.use('session')
          opts[:session] = entities.get(:session, session)
        end
        req = collection.find(args.use!('filter'), **opts)
        if batch_size = args.use('batchSize')
          req = req.batch_size(batch_size)
        end
        if sort = args.use('sort')
          req = req.sort(sort)
        end
        if limit = args.use('limit')
          req = req.limit(limit)
        end
        result = req.to_a
      end
    end

    def count_documents(op)
      collection = entities.get(:collection, op.use!('object'))
      use_arguments(op) do |args|
        collection.find(args.use!('filter')).count_documents
      end
    end

    def estimated_document_count(op)
      collection = entities.get(:collection, op.use!('object'))
      use_arguments(op) do |args|
        opts = {}
        if max_time_ms = args.use('maxTimeMS')
          opts[:max_time_ms] = max_time_ms
        end
        collection.estimated_document_count(**opts)
      end
    end

    def distinct(op)
      collection = entities.get(:collection, op.use!('object'))
      use_arguments(op) do |args|
        req = collection.find(args.use!('filter')).distinct(args.use!('fieldName'))
        result = req.to_a
      end
    end

    def find_one_and_update(op)
      collection = entities.get(:collection, op.use!('object'))
      use_arguments(op) do |args|
        filter = args.use!('filter')
        update = args.use!('update')
        opts = {}
        if return_document = args.use('returnDocument')
          opts[:return_document] = return_document.downcase.to_sym
        end
        collection.find_one_and_update(filter, update, **opts)
      end
    end

    def find_one_and_replace(op)
      collection = entities.get(:collection, op.use!('object'))
      use_arguments(op) do |args|
        filter = args.use!('filter')
        update = args.use!('replacement')
        collection.find_one_and_replace(filter, update)
      end
    end

    def find_one_and_delete(op)
      collection = entities.get(:collection, op.use!('object'))
      use_arguments(op) do |args|
        filter = args.use!('filter')
        collection.find_one_and_delete(filter)
      end
    end

    def insert_one(op)
      collection = entities.get(:collection, op.use!('object'))
      use_arguments(op) do |args|
        opts = {}
        if session = args.use('session')
          opts[:session] = entities.get(:session, session)
        end
        collection.insert_one(args.use!('document'), **opts)
      end
    end

    def insert_many(op)
      collection = entities.get(:collection, op.use!('object'))
      use_arguments(op) do |args|
        options = {}
        unless (ordered = args.use('ordered')).nil?
          options[:ordered] = ordered
        end
        collection.insert_many(args.use!('documents'), **options)
      end
    end

    def update_one(op)
      collection = entities.get(:collection, op.use!('object'))
      use_arguments(op) do |args|
        collection.update_one(args.use!('filter'), args.use!('update'))
      end
    end

    def update_many(op)
      collection = entities.get(:collection, op.use!('object'))
      use_arguments(op) do |args|
        collection.update_many(args.use!('filter'), args.use!('update'))
      end
    end

    def replace_one(op)
      collection = entities.get(:collection, op.use!('object'))
      use_arguments(op) do |args|
        collection.replace_one(
          args.use!('filter'),
          args.use!('replacement'),
          upsert: args.use('upsert'),
        )
      end
    end

    def delete_one(op)
      collection = entities.get(:collection, op.use!('object'))
      use_arguments(op) do |args|
        collection.delete_one(args.use!('filter'))
      end
    end

    def delete_many(op)
      collection = entities.get(:collection, op.use!('object'))
      use_arguments(op) do |args|
        collection.delete_many(args.use!('filter'))
      end
    end

    def bulk_write(op)
      collection = entities.get(:collection, op.use!('object'))
      use_arguments(op) do |args|
        requests = args.use!('requests').map do |req|
          convert_bulk_write_spec(req)
        end
        opts = {}
        if ordered = args.use('ordered')
          opts[:ordered] = true
        end
        collection.bulk_write(requests, **opts)
      end
    end

    private

    def convert_bulk_write_spec(spec)
      unless spec.keys.length == 1
        raise NotImplementedError, "Must have exactly one item"
      end
      op, spec = spec.first
      spec = UsingHash[spec]
      out = case op
      when 'insertOne'
        spec.use!('document')
      when 'updateOne', 'updateMany'
        {
          filter: spec.use('filter'),
          update: spec.use('update'),
          upsert: spec.use('upsert'),
        }
      when 'replaceOne'
        {
          filter: spec.use('filter'),
          replacement: spec.use('replacement'),
          upsert: spec.use('upsert'),
        }
      when 'deleteOne', 'deleteMany'
        {
          filter: spec.use('filter'),
        }
      else
        raise NotImplementedError, "Unknown operation #{op}"
      end
      unless spec.empty?
        raise NotImplementedError, "Unhandled keys: #{spec}"
      end
      {Utils.underscore(op) =>out}
    end

    def aggregate(op)
      obj = entities.get_any(op.use!('object'))
      args = op.use!('arguments')
      pipeline = args.use!('pipeline')
      unless args.empty?
        raise NotImplementedError, "Unhandled spec keys: #{test_spec}"
      end
      obj.aggregate(pipeline).to_a
    end
  end
end

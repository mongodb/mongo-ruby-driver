# frozen_string_literal: true
# rubocop:todo all

module Unified

  module CrudOperations

    def find(op)
      get_find_view(op).to_a
    end

    def find_one(op)
      get_find_view(op).first
    end

    def get_find_view(op)
      collection = entities.get(:collection, op.use!('object'))
      use_arguments(op) do |args|
        opts = {
          let: args.use('let'),
          comment: args.use('comment'),
          allow_disk_use: args.use('allowDiskUse'),
          show_disk_loc: args.use('showRecordId'),
          return_key: args.use('returnKey'),
          projection: args.use('projection'),
          skip: args.use('skip'),
          hint: args.use('hint'),
          max_value: args.use('max'),
          max_time_ms: args.use('maxTimeMS'),
          min_value: args.use('min'),
        }
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
        if projection = args.use('projection')
          req = req.projection(projection)
        end
        req
      end
    end

    def count_documents(op)
      collection = entities.get(:collection, op.use!('object'))
      use_arguments(op) do |args|
        opts = {}
        if session = args.use('session')
          opts[:session] = entities.get(:session, session)
        end
        if comment = args.use('comment')
          opts[:comment] = comment
        end
        collection.find(args.use!('filter')).count_documents(**opts)
      end
    end

    def estimated_document_count(op)
      collection = entities.get(:collection, op.use!('object'))
      use_arguments(op) do |args|
        opts = {}
        if max_time_ms = args.use('maxTimeMS')
          opts[:max_time_ms] = max_time_ms
        end
        if comment = args.use('comment')
          opts[:comment] = comment
        end
        collection.estimated_document_count(**opts)
      end
    end

    def distinct(op)
      collection = entities.get(:collection, op.use!('object'))
      use_arguments(op) do |args|
        opts = {}
        if session = args.use('session')
          opts[:session] = entities.get(:session, session)
        end
        if comment = args.use('comment')
          opts[:comment] = comment
        end
        req = collection.find(args.use!('filter'), **opts).distinct(args.use!('fieldName'), **opts)
        result = req.to_a
      end
    end

    def find_one_and_update(op)
      collection = entities.get(:collection, op.use!('object'))
      use_arguments(op) do |args|
        filter = args.use!('filter')
        update = args.use!('update')
        opts = {
          let: args.use('let'),
          comment: args.use('comment'),
          hint: args.use('hint'),
          upsert: args.use('upsert'),
        }
        if return_document = args.use('returnDocument')
          opts[:return_document] = return_document.downcase.to_sym
        end
        if session = args.use('session')
          opts[:session] = entities.get(:session, session)
        end
        collection.find_one_and_update(filter, update, **opts)
      end
    end

    def find_one_and_replace(op)
      collection = entities.get(:collection, op.use!('object'))
      use_arguments(op) do |args|
        filter = args.use!('filter')
        update = args.use!('replacement')
        opts = {
          let: args.use('let'),
          comment: args.use('comment'),
          hint: args.use('hint'),
        }
        if session = args.use('session')
          opts[:session] = entities.get(:session, session)
        end
        collection.find_one_and_replace(filter, update, **opts)
      end
    end

    def find_one_and_delete(op)
      collection = entities.get(:collection, op.use!('object'))
      use_arguments(op) do |args|
        filter = args.use!('filter')
        opts = {
          let: args.use('let'),
          comment: args.use('comment'),
          hint: args.use('hint'),
        }
        if session = args.use('session')
          opts[:session] = entities.get(:session, session)
        end
        collection.find_one_and_delete(filter, **opts)
      end
    end

    def insert_one(op)
      collection = entities.get(:collection, op.use!('object'))
      use_arguments(op) do |args|
        opts = {
          comment: args.use('comment')
        }
        if session = args.use('session')
          opts[:session] = entities.get(:session, session)
        end
        collection.insert_one(args.use!('document'), **opts)
      end
    end

    def insert_many(op)
      collection = entities.get(:collection, op.use!('object'))
      use_arguments(op) do |args|
        opts = {
          comment: args.use('comment')
        }
        unless (ordered = args.use('ordered')).nil?
          opts[:ordered] = ordered
        end
        if session = args.use('session')
          opts[:session] = entities.get(:session, session)
        end
        collection.insert_many(args.use!('documents'), **opts)
      end
    end

    def update_one(op)
      collection = entities.get(:collection, op.use!('object'))
      use_arguments(op) do |args|
        opts = {
          let: args.use('let'),
          comment: args.use('comment'),
          hint: args.use('hint'),
          upsert: args.use('upsert'),
        }
        if session = args.use('session')
          opts[:session] = entities.get(:session, session)
        end
        collection.update_one(args.use!('filter'), args.use!('update'), **opts)
      end
    end

    def update_many(op)
      collection = entities.get(:collection, op.use!('object'))
      use_arguments(op) do |args|
        opts = {
          let: args.use('let'),
          comment: args.use('comment'),
          hint: args.use('hint'),
        }
        collection.update_many(args.use!('filter'), args.use!('update'), **opts)
      end
    end

    def replace_one(op)
      collection = entities.get(:collection, op.use!('object'))
      use_arguments(op) do |args|
        collection.replace_one(
          args.use!('filter'),
          args.use!('replacement'),
          comment: args.use('comment'),
          upsert: args.use('upsert'),
          let: args.use('let'),
          hint: args.use('hint')
        )
      end
    end

    def delete_one(op)
      collection = entities.get(:collection, op.use!('object'))
      use_arguments(op) do |args|
        opts = {
          let: args.use('let'),
          comment: args.use('comment'),
          hint: args.use('hint'),
        }
        if session = args.use('session')
          opts[:session] = entities.get(:session, session)
        end
        collection.delete_one(args.use!('filter'), **opts)
      end
    end

    def delete_many(op)
      collection = entities.get(:collection, op.use!('object'))
      use_arguments(op) do |args|
        opts = {
          let: args.use('let'),
          comment: args.use('comment'),
          hint: args.use('hint'),
        }
        collection.delete_many(args.use!('filter'), **opts)
      end
    end

    def bulk_write(op)
      collection = entities.get(:collection, op.use!('object'))
      use_arguments(op) do |args|
        requests = args.use!('requests').map do |req|
          convert_bulk_write_spec(req)
        end
        opts = {}
        if args.key?('ordered')
          opts[:ordered] = args.use!('ordered')
        end
        if comment = args.use('comment')
          opts[:comment] = comment
        end
        if let = args.use('let')
          opts[:let] = let
        end
        collection.bulk_write(requests, **opts)
      end
    end

    def aggregate(op)
      obj = entities.get_any(op.use!('object'))
      args = op.use!('arguments')
      pipeline = args.use!('pipeline')
      opts = {
        let: args.use('let'),
      }
      if session = args.use('session')
        opts[:session] = entities.get(:session, session)
      end
      if comment = args.use('comment')
        opts[:comment] = comment
      end
      if batch_size = args.use('batchSize')
        opts[:batch_size] = batch_size
      end
      if args.key?('allowDiskUse')
        opts[:allow_disk_use] = args.use('allowDiskUse')
      end
      unless args.empty?
        raise NotImplementedError, "Unhandled spec keys: #{args} in #{test_spec}"
      end
      obj.aggregate(pipeline, **opts).to_a
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
          array_filters: spec.use('arrayFilters'),
          hint: spec.use('hint'),
        }
      when 'replaceOne'
        {
          filter: spec.use('filter'),
          replacement: spec.use('replacement'),
          upsert: spec.use('upsert'),
          hint: spec.use('hint'),
        }
      when 'deleteOne', 'deleteMany'
        {
          filter: spec.use('filter'),
          hint: spec.use('hint'),
        }
      else
        raise NotImplementedError, "Unknown operation #{op}"
      end
      unless spec.empty?
        raise NotImplementedError, "Unhandled keys: #{spec}"
      end
      {Utils.underscore(op) =>out}
    end
  end
end

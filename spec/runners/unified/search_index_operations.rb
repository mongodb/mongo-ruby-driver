# frozen_string_literal: true

module Unified
  # The definitions of available search index operations, as used by the
  # unified tests.
  module SearchIndexOperations
    def create_search_index(op)
      collection = entities.get(:collection, op.use!('object'))

      use_arguments(op) do |args|
        model = args.use('model')
        name = model.use('name')
        definition = model.use('definition')
        collection.search_indexes.create_one(definition, name: name)
      end
    end

    def create_search_indexes(op)
      collection = entities.get(:collection, op.use!('object'))

      use_arguments(op) do |args|
        models = args.use('models')
        collection.search_indexes.create_many(models)
      end
    end

    def drop_search_index(op)
      collection = entities.get(:collection, op.use!('object'))

      use_arguments(op) do |args|
        collection.search_indexes.drop_one(
          id: args.use('id'),
          name: args.use('name')
        )
      end
    end

    def list_search_indexes(op)
      collection = entities.get(:collection, op.use!('object'))

      use_arguments(op) do |args|
        agg_opts = args.use('aggregationOptions') || {}
        collection.search_indexes(
          id: args.use('id'),
          name: args.use('name'),
          aggregate: ::Utils.underscore_hash(agg_opts)
        ).to_a
      end
    end

    def update_search_index(op)
      collection = entities.get(:collection, op.use!('object'))

      use_arguments(op) do |args|
        collection.search_indexes.update_one(
          args.use('definition'),
          id: args.use('id'),
          name: args.use('name')
        )
      end
    end
  end
end

# frozen_string_literal: true

module Mongo
  module SearchIndex
    # A class representing a view of search indexes.
    class View
      include Enumerable
      include Retryable

      # @return [ Mongo::Collection ] the collection this view belongs to
      attr_reader :collection

      # @return [ nil | String ] the index id to query
      attr_reader :requested_index_id

      # @return [ nil | String ] the index name to query
      attr_reader :requested_index_name

      # @return [ nil | Integer ] the batch size to use for the aggregation
      #   pipeline
      attr_reader :batch_size

      # Create the new search index view.
      #
      # @param [ Collection ] collection The collection.
      # @param [ Hash ] options The options that configure the behavior of the view.
      #
      # @option options [ String ] :id The specific index id to query (optional)
      # @option options [ String ] :name The name of the specific index to query (optional)
      # @option options [ Integer ] :batch_size The batch size to use for
      #    returning the indexes (optional)
      def initialize(collection, options = {})
        @collection = collection
        @requested_index_id = options[:id]
        @requested_index_name = options[:name]
        @batch_size = options[:batch_size]
      end

      def create_one(definition, name: nil)
        doc = validate_search_index!({ name: name, definition: definition })
        create_many([ doc ]).first
      end

      def create_many(defs)
        spec = spec_with(indexes: defs.map { |v| validate_search_index!(v) })
        Operation::CreateSearchIndexes.new(spec).execute(server, context: execution_context)
      end

      def drop(id: nil, name: nil)
        validate_id_or_name!(id, name)

        spec = spec_with(index_id: id, index_name: name)
        Operation::DropSearchIndex.new(spec).execute(server, context: execution_context)
      end

      def each(&block)
        spec = {}.tap do |s|
          s[:id] = requested_index_id if requested_index_id
          s[:name] = requested_index_name if requested_index_name
        end

        result = collection.aggregate(
          [ { '$listSearchIndexes' => spec } ],
          batch_size: batch_size
        )

        return result.to_enum unless block

        result.each(&block)
        self
      end

      def update(definition, id: nil, name: nil)
        validate_id_or_name!(id, name)

        spec = spec_with(index_id: id, index_name: name, index: definition)
        Operation::UpdateSearchIndex.new(spec).execute(server, context: execution_context)
      end

      private

      def spec_with(extras)
        {
          coll_name: collection.name,
          db_name: collection.database.name,
        }.merge(extras)
      end

      def server
        collection.cluster.next_primary
      end

      def execution_context
        Operation::Context.new(client: collection.client)
      end

      def validate_id_or_name!(id, name)
        return unless (id.nil? && name.nil?) || (!id.nil? && !name.nil?)

        raise ArgumentError, 'exactly one of id or name must be specified'
      end

      def validate_search_index!(doc)
        validate_search_index_keys!(doc.keys)
        validate_search_index_name!(doc[:name] || doc['name'])
        validate_search_index_definition!(doc[:definition] || doc['definition'])
        doc
      end

      def validate_search_index_keys!(keys)
        extras = keys - [ 'name', 'definition', :name, :definition ]

        raise ArgumentError, "invalid keys in search index creation: #{extras.inspect}" if extras.any?
      end

      def validate_search_index_name!(name)
        return if name.nil? || name.is_a?(String)

        raise ArgumentError, "search index name must be nil or a string (got #{name.inspect})"
      end

      def validate_search_index_definition!(definition)
        return if definition.is_a?(Hash)

        raise ArgumentError, "search index definition must be a Hash (got #{definition.inspect})"
      end
    end
  end
end

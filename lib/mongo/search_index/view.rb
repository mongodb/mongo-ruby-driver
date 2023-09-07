# frozen_string_literal: true

module Mongo
  module SearchIndex
    # A class representing a view of search indexes.
    class View
      include Enumerable
      include Retryable
      include Collection::Helpers

      # @return [ Mongo::Collection ] the collection this view belongs to
      attr_reader :collection

      # @return [ nil | String ] the index id to query
      attr_reader :requested_index_id

      # @return [ nil | String ] the index name to query
      attr_reader :requested_index_name

      # @return [ Hash ] the options hash to use for the aggregate command
      #   when querying the available indexes.
      attr_reader :aggregate_options

      # Create the new search index view.
      #
      # @param [ Collection ] collection The collection.
      # @param [ Hash ] options The options that configure the behavior of the view.
      #
      # @option options [ String ] :id The specific index id to query (optional)
      # @option options [ String ] :name The name of the specific index to query (optional)
      # @option options [ Hash ] :aggregate The options hash to send to the
      #   aggregate command when querying the available indexes.
      def initialize(collection, options = {})
        @collection = collection
        @requested_index_id = options[:id]
        @requested_index_name = options[:name]
        @aggregate_options = options[:aggregate] || {}

        return if @aggregate_options.is_a?(Hash)

        raise ArgumentError, "The :aggregate option must be a Hash (got a #{@aggregate_options.class})"
      end

      # Create a single search index with the given definition. If the name is
      # provided, the new index will be given that name.
      #
      # @param [ Hash ] definition The definition of the search index.
      # @param [ nil | String ] name The name to give the new search index.
      #
      # @return [ String ] the name of the new search index.
      def create_one(definition, name: nil)
        create_many([ { name: name, definition: definition } ]).first
      end

      # Create multiple search indexes with a single command.
      #
      # @param [ Array<Hash> ] indexes The description of the indexes to
      #   create. Each element of the list must be a hash with a definition
      #   key, and an optional name key.
      #
      # @return [ Array<String> ] the names of the new search indexes.
      def create_many(indexes)
        spec = spec_with(indexes: indexes.map { |v| validate_search_index!(v) })
        result = Operation::CreateSearchIndexes.new(spec).execute(next_primary, context: execution_context)
        result.first['indexesCreated'].map { |idx| idx['name'] }
      end

      # Drop the search index with the given id, or name. One or the other must
      # be specified, but not both.
      #
      # @param [ String ] id the id of the index to drop
      # @param [ String ] name the name of the index to drop
      #
      # @return [ Mongo::Operation::Result | false ] the result of the
      #    operation, or false if the given index does not exist.
      def drop_one(id: nil, name: nil)
        validate_id_or_name!(id, name)

        spec = spec_with(index_id: id, index_name: name)
        op = Operation::DropSearchIndex.new(spec)

        # per the spec:
        # Drivers MUST suppress NamespaceNotFound errors for the
        # ``dropSearchIndex`` helper.  Drop operations should be idempotent.
        do_drop(op, nil, execution_context)
      end

      # Iterate over the search indexes.
      #
      # @param [ Proc ] block if given, each search index will be yieleded to
      #    the block.
      #
      # @return [ self | Enumerator ] if a block is given, self is returned.
      #    Otherwise, an enumerator will be returned.
      def each(&block)
        @result ||= begin
          spec = {}.tap do |s|
            s[:id] = requested_index_id if requested_index_id
            s[:name] = requested_index_name if requested_index_name
          end

          collection.aggregate(
            [ { '$listSearchIndexes' => spec } ],
            aggregate_options
          )
        end

        return @result.to_enum unless block

        @result.each(&block)
        self
      end

      # Update the search index with the given id or name. One or the other
      # must be provided, but not both.
      #
      # @param [ Hash ] definition the definition to replace the given search
      #    index with.
      # @param [ nil | String ] id the id of the search index to update
      # @param [ nil | String ] name the name of the search index to update
      #
      # @return [ Mongo::Operation::Result ] the result of the operation
      def update_one(definition, id: nil, name: nil)
        validate_id_or_name!(id, name)

        spec = spec_with(index_id: id, index_name: name, index: definition)
        Operation::UpdateSearchIndex.new(spec).execute(next_primary, context: execution_context)
      end

      # The following methods are to make the view act more like an array,
      # without having to explicitly make it an array...

      # Queries whether the search index enumerable is empty.
      #
      # @return [ true | false ] whether the enumerable is empty or not.
      def empty?
        count.zero?
      end

      private

      # A helper method for building the specification document with certain
      # values pre-populated.
      #
      # @param [ Hash ] extras the values to put into the specification
      #
      # @return [ Hash ] the specification document
      def spec_with(extras)
        {
          coll_name: collection.name,
          db_name: collection.database.name,
        }.merge(extras)
      end

      # A helper method for retrieving the primary server from the cluster.
      #
      # @return [ Mongo::Server ] the server to use
      def next_primary(ping = nil, session = nil)
        collection.cluster.next_primary(ping, session)
      end

      # A helper method for constructing a new operation context for executing
      # an operation.
      #
      # @return [ Mongo::Operation::Context ] the operation context
      def execution_context
        Operation::Context.new(client: collection.client)
      end

      # Validates the given id and name, ensuring that exactly one of them
      # is non-nil.
      #
      # @param [ nil | String ] id the id to validate
      # @param [ nil | String ] name the name to validate
      #
      # @raise [ ArgumentError ] if neither or both arguments are nil
      def validate_id_or_name!(id, name)
        return unless (id.nil? && name.nil?) || (!id.nil? && !name.nil?)

        raise ArgumentError, 'exactly one of id or name must be specified'
      end

      # Validates the given search index document, ensuring that it has no
      # extra keys, and that the name and definition are valid.
      #
      # @param [ Hash ] doc the document to validate
      #
      # @raise [ ArgumentError ] if the document is invalid.
      def validate_search_index!(doc)
        validate_search_index_keys!(doc.keys)
        validate_search_index_name!(doc[:name] || doc['name'])
        validate_search_index_definition!(doc[:definition] || doc['definition'])
        doc
      end

      # Validates the keys of a search index document, ensuring that
      # they are all valid.
      #
      # @param [ Array<String | Hash> ] keys the keys of a search index document
      #
      # @raise [ ArgumentError ] if the list contains any invalid keys
      def validate_search_index_keys!(keys)
        extras = keys - [ 'name', 'definition', :name, :definition ]

        raise ArgumentError, "invalid keys in search index creation: #{extras.inspect}" if extras.any?
      end

      # Validates the name of a search index, ensuring that it is either a
      # String or nil.
      #
      # @param [ nil | String ] name the name of a search index
      #
      # @raise [ ArgumentError ] if the name is not valid
      def validate_search_index_name!(name)
        return if name.nil? || name.is_a?(String)

        raise ArgumentError, "search index name must be nil or a string (got #{name.inspect})"
      end

      # Validates the definition of a search index.
      #
      # @param [ Hash ] definition the definition of a search index
      #
      # @raise [ ArgumentError ] if the definition is not valid
      def validate_search_index_definition!(definition)
        return if definition.is_a?(Hash)

        raise ArgumentError, "search index definition must be a Hash (got #{definition.inspect})"
      end
    end
  end
end

module Mongo
  module CRUD

    # Represents a single CRUD test.
    #
    # @since 2.0.0
    class CRUDTest < CRUDTestBase

      # The test description.
      #
      # @return [ String ] description The test description.
      #
      # @since 2.0.0
      attr_reader :description

      # Spec tests have configureFailPoint as a string, make it a string here too
      FAIL_POINT_BASE_COMMAND = BSON::Document.new(
        'configureFailPoint' => "onPrimaryTransactionalWrite",
      ).freeze

      # Instantiate the new CRUDTest.
      #
      # data can be an array of hashes, with each hash corresponding to a
      # document to be inserted into the collection whose name is given in
      # collection_name as configured in the YAML file. Alternatively data
      # can be a map of collection names to arrays of hashes.
      #
      # @param [ Crud::Spec ] crud_spec The top level YAML specification object.
      # @param [ Hash | Array<Hash> ] data The documents the collection
      #   must have before the test runs.
      # @param [ Hash ] test The test specification.
      #
      # @since 2.0.0
      def initialize(crud_spec, data, test)
        @spec = crud_spec
        @data = data
        if test['failPoint']
          @fail_point_command = FAIL_POINT_BASE_COMMAND.merge(test['failPoint'])
        end
        @description = test['description']
        @client_options = Utils.convert_client_options(test['clientOptions'] || {})
        if test['operations']
          @operations = test['operations'].map do |op_spec|
            Operation.new(self, op_spec)
          end
        else
          @operations = [Operation.new(self, test['operation'], test['outcome'])]
        end
        @expectations = test['expectations']
      end

      attr_reader :client_options

      # Operations to be performed by the test.
      #
      # For CRUD tests, there is one operation for test. For retryable writes,
      # there are multiple operations for each test. In either case we build
      # an array of operations.
      attr_reader :operations

      # The expected command monitoring events
      attr_reader :expectations

      # Run the test.
      #
      # The specified number of operations are executed, so that the
      # test can assert on the outcome of each specified operation in turn.
      #
      # @param [ Client ] client The client the test
      #   should be run with.
      # @param [ Integer ] num_ops Number of operations to run.
      #
      # @return [ Result, Array<Hash> ] The result(s) of running the test.
      #
      # @since 2.0.0
      def run(client, num_ops)
        result = nil
        1.upto(num_ops) do |i|
          operation = @operations[i-1]
          target = resolve_target(client, operation)
          result = operation.execute(target)
        end
        result
      end

      class DataConverter
        include Mongo::GridFS::Convertible
      end

      def setup_test(spec, client)
        clear_fail_point(client)
        if @data.nil?
          # nothing to do
        elsif @data.is_a?(Array)
          collection = client[spec.collection_name]
          collection.delete_many
          collection.insert_many(@data)
        elsif @data.is_a?(Hash)
          converter = DataConverter.new
          @data.each do |collection_name, data|
            collection = client[collection_name]
            collection.delete_many
            data = converter.transform_docs(data)
            collection.insert_many(data)
          end
        else
          raise "Unknown type of data: #{@data}"
        end
        setup_fail_point(client)
      end

      def setup_fail_point(client)
        if @fail_point_command
          client.use(:admin).command(@fail_point_command)
        end
      end

      def clear_fail_point(client)
        if @fail_point_command
          client.use(:admin).command(@fail_point_command.merge(mode: "off"))
        end
      end

      private

      def actual_collection_data
        if expected_outcome.collection_data?
          collection_name = expected_outcome.collection_name || @collection.name
          @collection.database[collection_name].find.to_a
        end
      end
    end
  end
end

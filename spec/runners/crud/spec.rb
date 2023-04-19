# frozen_string_literal: true
# rubocop:todo all

module Mongo
  module CRUD
    # Represents a CRUD specification test.
    class Spec

      # Instantiate the new spec.
      #
      # @param [ String ] test_path The path to the file.
      #
      # @since 2.0.0
      def initialize(test_path)
        @spec = ::Utils.load_spec_yaml_file(test_path)
        @description = File.basename(test_path)
        @data = BSON::ExtJSON.parse_obj(@spec['data'])
        @tests = @spec['tests']

        # Introduced with Client-Side Encryption tests
        @json_schema = BSON::ExtJSON.parse_obj(@spec['json_schema'])
        @key_vault_data = BSON::ExtJSON.parse_obj(@spec['key_vault_data'])
        @encrypted_fields = BSON::ExtJSON.parse_obj(@spec['encrypted_fields'], mode: :bson)

        @requirements = if run_on = @spec['runOn']
          run_on.map do |spec|
            Requirement.new(spec)
          end
        elsif Requirement::YAML_KEYS.any? { |key| @spec.key?(key) }
          [Requirement.new(@spec)]
        else
          nil
        end
      end

      # @return [ String ] description The spec description.
      #
      # @since 2.0.0
      attr_reader :description

      attr_reader :requirements

      # @return [ Hash ] The jsonSchema collection validator.
      attr_reader :json_schema

      # @return [ Array<Hash> ] Data to insert into the key vault before
      #   running each test.
      attr_reader :key_vault_data

      # @return [ Hash ]  An encryptedFields option that should be set on the
      #   collection (using createCollection) before each test run.
      attr_reader :encrypted_fields

      def collection_name
        # Older spec tests do not specify a collection name, thus
        # we provide a default here
        @spec['collection_name'] || 'crud_spec_test'
      end

      def bucket_name
        @spec['bucket_name']
      end

      def database_name
        @spec['database_name']
      end

      # Get a list of Test instances, one for each test definition.
      def tests
        @tests.map do |test|
          Mongo::CRUD::CRUDTest.new(self, @data, test)
        end
      end
    end
  end
end

module Mongo
  module CRUD
    # Represents a CRUD specification test.
    class Spec

      # Instantiate the new spec.
      #
      # @example Create the spec.
      #   Spec.new(file)
      #
      # @param [ String ] file The name of the file.
      #
      # @since 2.0.0
      def initialize(file)
        contents = ERB.new(File.read(file)).result

        # Since Ruby driver binds a client to a database, change the
        # database name in the spec to the one we are using
        contents.sub!(/"retryable-reads-tests"/, '"ruby-driver"')
        contents.sub!(/"transaction-tests"/, '"ruby-driver"')
        contents.sub!(/"withTransaction-tests"/, '"ruby-driver"')

        @spec = YAML.load(ERB.new(contents).result)
        @description = File.basename(file)
        @data = @spec['data']
        @tests = @spec['tests']

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
          Mongo::CRUD::CRUDTest.new(@data, test)
        end
      end
    end
  end
end

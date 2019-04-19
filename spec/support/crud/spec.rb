module Mongo
  module CRUD
    # Represents a CRUD specification test.
    #
    # @since 2.0.0
    class Spec < SpecBase

      # @return [ String ] description The spec description.
      #
      # @since 2.0.0
      attr_reader :description

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

        @spec = YAML.load(ERB.new(contents).result)
        @description = File.basename(file)
        @data = @spec['data']
        @tests = @spec['tests']
        @collection_name = @spec['collection_name']
        @bucket_name = @spec['bucket_name']

        super()
      end

      def collection_name
        @collection_name || 'crud_spec_test'
      end

      attr_reader :bucket_name

      # Get a list of CRUDTests for each test definition.
      #
      # @example Get the list of CRUDTests.
      #   spec.tests
      #
      # @return [ Array<CRUDTest> ] The list of CRUDTests.
      #
      # @since 2.0.0
      def tests
        @tests.map do |test|
          Mongo::CRUD::CRUDTest.new(@data, test)
        end
      end
    end
  end
end

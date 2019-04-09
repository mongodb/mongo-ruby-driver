module Mongo
  module CRUD
    # Represents a CRUD specification test.
    #
    # @since 2.0.0
    class Spec

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
        file = File.new(file)
        @spec = YAML.load(ERB.new(file.read).result)
        file.close
        @description = File.basename(file)
        @data = @spec['data']
        @crud_tests = @spec['tests']
        @requirements = Requirement.new(@spec)
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

      attr_reader :requirements

      # Whether the test can be run on a given server version.
      #
      # @example Can the test run on this server version?
      #   spec.server_version_satisfied?(client)
      #
      # @param [ Mongo::Client ] client The client to check.
      #
      # @return [ true, false ] Whether the test can be run on the given
      #   server version.
      #
      # @since 2.4.0
      def server_version_satisfied?(client)
        lower_bound_satisfied?(client) && upper_bound_satisfied?(client)
      end

      # Get a list of CRUDTests for each test definition.
      #
      # @example Get the list of CRUDTests.
      #   spec.tests
      #
      # @return [ Array<CRUDTest> ] The list of CRUDTests.
      #
      # @since 2.0.0
      def tests
        @crud_tests.collect do |test|
          Mongo::CRUD::CRUDTest.new(@data, test)
        end
      end

      private

      def upper_bound_satisfied?(client)
        return true unless @max_server_version
        client.database.command(buildInfo: 1).first['version'] <= @max_server_version
      end

      def lower_bound_satisfied?(client)
        return true unless @min_server_version
        #@min_server_version <= client.database.command(buildInfo: 1).first['version']
        @min_server_version <= ClusterConfig.instance.fcv_ish
      end
    end
  end
end

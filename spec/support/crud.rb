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
        @spec = YAML.load(ERB.new(File.new(file).read).result)
        @description = file
        @data = @spec['data']
        @crud_tests = @spec['tests']
      end

      def tests
        @crud_tests.collect do |test|
          Mongo::CRUD::CRUDTest.new(@data, test)
        end
      end
    end

    class CRUDTest

      attr_reader :description

      def initialize(data, test)
        @data = data
        @description = test['description']
        @operation = Operation.new(test['operation'])
        @outcome = test['outcome']
      end

      def run(collection)
        collection.find.delete_many
        collection.insert_many(@data)
        @operation.run(collection)
      end

      def result
        @outcome['result']
      end
    end

    class Operation

      ARGUMENTS = {
                    'sort' => :sort,
                    'skip' => :skip,
                    'batchSize' => :batch_size,
                    'limit' => :limit
                  }

      attr_reader :name

      def initialize(spec)
        @spec = spec
        @name = @spec['name']
      end

      def find(collection)
        view = collection.find(filter)
        arguments.each do |key, value|
          view = view.send(ARGUMENTS[key], value) unless key == 'filter'
        end
        view.to_a
      end

      def filter
        arguments['filter']
      end

      def arguments
        @spec['arguments']
      end

      def run(collection)
        send(name.to_sym, collection)
      end
    end
  end
end

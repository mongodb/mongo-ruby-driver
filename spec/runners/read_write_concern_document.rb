# frozen_string_literal: true
# rubocop:todo all

module ReadWriteConcernDocument

  class Spec

    attr_reader :description

    # Instantiate the new spec.
    #
    # @param [ String ] test_path The path to the file.
    #
    # @since 2.0.0
    def initialize(test_path)
      @spec = ::Utils.load_spec_yaml_file(test_path)
      @description = File.basename(test_path)
    end

    def tests
      @tests ||= @spec['tests'].collect do |spec|
        Test.new(spec)
      end
    end
  end

  class Test
    def initialize(spec)
      @spec = spec
      @description = @spec['description']
      @uri_string = @spec['uri']
    end

    attr_reader :description

    def valid?
      !!@spec['valid']
    end

    def input_document
      (@spec['readConcern'] || @spec['writeConcern']).tap do |concern|
        # Documented Ruby API matches the server API, and Ruby prohibits
        # journal key as used in the spec tests...
        if concern.key?('journal')
          concern['j'] = concern.delete('journal')
        end
        # ... and uses wtimeout instead of wtimeoutMS
        if concern.key?('wtimeoutMS')
          concern['wtimeout'] = concern.delete('wtimeoutMS')
        end
      end
    end

    def server_document
      @spec['readConcernDocument'] || @spec['writeConcernDocument']
    end

    # Returns true, false or nil
    def server_default?
      # Do not convert to boolean
      @spec['isServerDefault']
    end

    # Returns true, false or nil
    def acknowledged?
      # Do not convert to boolean
      @spec['isAcknowledged']
    end
  end
end

module Mongo
  module SDAM

    class Spec

      attr_reader :description
      attr_reader :phases
      attr_reader :uri
      attr_reader :uri_string

      def initialize(file)
        @test = YAML.load(ERB.new(File.new(file).read).result)
        @description = @test['description']
        @uri_string = @test['uri']
        @uri = URI.new(uri_string)
        @phases = @test['phases'].map{ |phase| Phase.new(phase, uri) }
      end
    end

    class Phase

      attr_reader :outcome
      attr_reader :responses

      def initialize(phase, uri)
        @phase = phase
        @responses = @phase['responses'].map{ |response| Response.new(response, uri) }
        @outcome = Outcome.new(@phase['outcome'])
      end
    end

    class Response

      attr_reader :address
      attr_reader :ismaster

      def initialize(response, uri)
        @uri = uri
        @address = response[0]
        @ismaster = response[1]
      end

      def original_address
        @uri.servers.detect{ |server| address.start_with?(server) } || address
      end
    end

    class Outcome

      attr_reader :servers
      attr_reader :set_name
      attr_reader :topology_type

      def initialize(outcome)
        @servers = outcome['servers']
        @set_name = outcome['setName']
        @topology_type = outcome['topologyType']
      end
    end
  end
end

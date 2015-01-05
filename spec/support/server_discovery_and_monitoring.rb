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
        @phases = @test['phases'].map{ |phase| Phase.new(phase) }
        @uri_string = @test['uri']
        @uri = URI.new(uri_string)
      end
    end

    class Phase

      attr_reader :outcome
      attr_reader :responses

      def initialize(phase)
        @phase = phase
        @responses = @phase['responses'].map{ |response| Response.new(response) }
        @outcome = Outcome.new(@phase['outcome'])
      end
    end

    class Response

      def initialize(response)
        @address = response[0]
        @ismaster = response[1]
      end

      def reply
        message = Mongo::Protocol::Reply.new
        message.instance_variable_set(:@documents, [ @ismaster ])
        message
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

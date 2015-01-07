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

    class Server
      extend Forwardable

      attr_reader :address

      def_delegators :@description,
                     :features,
                     :ghost?,
                     :max_wire_version,
                     :max_write_batch_size,
                     :max_bson_object_size,
                     :max_message_size,
                     :mongos?,
                     :primary?,
                     :replica_set_name,
                     :secondary?,
                     :standalone?,
                     :unknown?

      def initialize(address, event_listeners, options = {}, ismaster)
        @address = Mongo::Server::Address.new(address)
        @options = options.freeze
        @description = Mongo::Server::Description.new({}, event_listeners)
        @description.update!(ismaster, 0.5)
      end

      def inspect
        "#<Mongo::SDAM::Server:0x#{object_id} address=#{address.host}:#{address.port}>"
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

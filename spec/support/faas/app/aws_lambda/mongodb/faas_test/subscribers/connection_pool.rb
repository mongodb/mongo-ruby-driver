# frozen_string_literal: true

module FaaSTest
  module Subscribers
    class ConnectionPool
      attr_reader :open_connections

      def initialize
        @open_connections = 0
      end

      def published(event)
        case event
        when Mongo::Monitoring::Event::Cmap::ConnectionCreated
          @open_connections += 1
        when Mongo::Monitoring::Event::Cmap::ConnectionClosed
          @open_connections -= 1
        end
      end
    end
  end
end

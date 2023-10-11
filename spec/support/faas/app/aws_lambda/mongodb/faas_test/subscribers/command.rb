# frozen_string_literal: true

module FaaSTest
  module Subscribers
    class Command
      attr_reader :durations

      def initialize
        @durations = []
      end

      def started(_event)
      end

      def succeeded(event)
        @durations.push event.duration
      end

      def failed(_event)
      end
    end
  end
end

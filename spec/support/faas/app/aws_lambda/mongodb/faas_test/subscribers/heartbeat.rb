# frozen_string_literal: true

module FaaSTest
  module Subscribers
    class Heartbeat
      attr_reader :durations
      attr_reader :started_count, :succeeded_count, :failed_count

      def initialize
        @durations = []
        @started_count = @succeeded_count = @failed_count = 0
      end

      def started(_event)
        @started_count += 1
      end

      def succeeded(event)
        @succeeded_count += 1
        @durations.push event.round_trip_time
      end

      def failed(_event)
        @failed_count += 1
      end
    end
  end
end

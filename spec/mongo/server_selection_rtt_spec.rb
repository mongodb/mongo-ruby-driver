require 'spec_helper'

describe 'Server Selection moving average round trip time calculation' do

  include Mongo::ServerSelection::RTT

  SERVER_SELECTION_RTT_TESTS.each do |file|

    spec = Mongo::ServerSelection::RTT::Spec.new(file)

    before(:all) do

      # We monkey-patch the monitor here, so the history of rtt's can be controlled.
      # We keep the API of Monitor#initialize but add in an extra option and seed the rtt history.
      #
      # @since 2.0.0
      class Mongo::Server::Monitor

        def initialize(address, listeners, options = {})
          @description = Mongo::Server::Description.new(address, {}, options[:avg_rtt_ms])
          @inspector = Mongo::Server::Description::Inspector.new(listeners)
          @options = options.freeze
          @connection = Connection.new(address, options)
          @round_trip_times = @description.average_round_trip_time ? [ @description.average_round_trip_time ] : []
          @mutex = Mutex.new
        end

        private

        # We monkey patch this method to use an instance variable instead of calculating time elapsed.
        #
        # @since 2.0.0
        def average_round_trip_time(start)
          new_rtt = @new_rtt_ms
          return new_rtt unless last_round_trip_time
          average = RTT_WEIGHT_FACTOR * new_rtt + (1 - RTT_WEIGHT_FACTOR) * last_round_trip_time
          @round_trip_times.push(new_rtt)
          average
        end
      end
    end

    after(:all) do

      # Return the monitor implementation to its original for the other
      # tests in the suite.
      class Mongo::Server::Monitor

        # Create the new server monitor.
        #
        # @example Create the server monitor.
        #   Mongo::Server::Monitor.new(address, listeners)
        #
        # @param [ Address ] address The address to monitor.
        # @param [ Event::Listeners ] listeners The event listeners.
        # @param [ Hash ] options The options.
        #
        # @since 2.0.0
        def initialize(address, listeners, options = {})
          @description = Description.new(address, {})
          @inspector = Description::Inspector.new(listeners)
          @options = options.freeze
          @connection = Connection.new(address, options)
          @round_trip_times = []
          @mutex = Mutex.new
        end

        private

        def average_round_trip_time(start)
          new_rtt = Time.now - start
          return new_rtt unless last_round_trip_time
          average = RTT_WEIGHT_FACTOR * new_rtt + (1 - RTT_WEIGHT_FACTOR) * last_round_trip_time
          @round_trip_times.push(new_rtt)
          average
        end
      end
    end

    context(spec.description) do

      let(:address) do
        Mongo::Address.new('127.0.0.1:27017')
      end

      let(:monitor) do
        Mongo::Server::Monitor.new(address, Mongo::Event::Listeners.new, avg_rtt_ms: spec.avg_rtt_ms)
      end

      before do
        monitor.instance_variable_set(:@new_rtt_ms, spec.new_rtt_ms)
        monitor.scan!
      end

      it 'correctly caculates the moving average round trip time' do
        expect(monitor.description.average_round_trip_time).to eq(spec.new_avg_rtt)
      end
    end
  end
end

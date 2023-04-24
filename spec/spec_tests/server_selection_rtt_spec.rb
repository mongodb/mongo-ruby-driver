# frozen_string_literal: true
# rubocop:todo all

require 'spec_helper'

require 'runners/server_selection_rtt'

describe 'Server Selection moving average round trip time calculation' do

  include Mongo::ServerSelection::RTT

  SERVER_SELECTION_RTT_TESTS.each do |file|

    spec = Mongo::ServerSelection::RTT::Spec.new(file)

    context(spec.description) do

      let(:averager) do
        Mongo::Server::RoundTripTimeAverager.new
      end

      before do
        averager.instance_variable_set(:@average_round_trip_time, spec.average_rtt)
        averager.instance_variable_set(:@last_round_trip_time, spec.new_rtt)
        averager.send(:update_average_round_trip_time)
      end

      it 'correctly calculates the moving average round trip time' do
        expect(averager.average_round_trip_time).to eq(spec.new_average_rtt)
      end
    end
  end
end

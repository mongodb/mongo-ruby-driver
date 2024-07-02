# frozen_string_literal: true
# rubocop:todo all

require 'spec_helper'

require 'runners/server_selection_rtt'

describe 'Server Selection moving average round trip time calculation' do

  include Mongo::ServerSelection::RTT

  SERVER_SELECTION_RTT_TESTS.each do |file|

    spec = Mongo::ServerSelection::RTT::Spec.new(file)

    context(spec.description) do

      let(:calculator) do
        Mongo::Server::RoundTripTimeCalculator.new
      end

      before do
        calculator.instance_variable_set(:@average_round_trip_time, spec.average_rtt)
        calculator.instance_variable_set(:@last_round_trip_time, spec.new_rtt)
        calculator.update_average_round_trip_time
      end

      it 'correctly calculates the moving average round trip time' do
        expect(calculator.average_round_trip_time).to eq(spec.new_average_rtt)
      end
    end
  end
end

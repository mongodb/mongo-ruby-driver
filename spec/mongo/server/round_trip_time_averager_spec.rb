# frozen_string_literal: true
# rubocop:todo all

require 'spec_helper'

describe Mongo::Server::RoundTripTimeAverager do
  let(:averager) { Mongo::Server::RoundTripTimeAverager.new }

  describe '#update_average_round_trip_time' do
    context 'no existing average rtt' do
      it 'updates average rtt' do
        averager.instance_variable_set('@last_round_trip_time', 5)
        averager.send(:update_average_round_trip_time)
        expect(averager.average_round_trip_time).to eq(5)
      end
    end

    context 'with existing average rtt' do
      it 'averages with existing average rtt' do
        averager.instance_variable_set('@last_round_trip_time', 5)
        averager.instance_variable_set('@average_round_trip_time', 10)
        averager.send(:update_average_round_trip_time)
        expect(averager.average_round_trip_time).to eq(9)
      end
    end
  end

  describe '#measure' do
    context 'block does not raise' do
      it 'updates average rtt' do
        expect(averager).to receive(:update_average_round_trip_time)
        averager.measure do
        end
      end
    end

    context 'block raises' do
      it 'does not update average rtt' do
        expect(averager).not_to receive(:update_average_round_trip_time)
        lambda do
          averager.measure do
            raise "Problem"
          end
        end.should raise_error(/Problem/)
      end
    end
  end
end

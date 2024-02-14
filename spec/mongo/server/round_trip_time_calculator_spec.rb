# frozen_string_literal: true
# rubocop:todo all

require 'spec_helper'

describe Mongo::Server::RoundTripTimeCalculator do
  let(:calculator) { Mongo::Server::RoundTripTimeCalculator.new }

  describe '#update_average_round_trip_time' do
    context 'no existing average rtt' do
      it 'updates average rtt' do
        calculator.instance_variable_set('@last_round_trip_time', 5)
        calculator.update_average_round_trip_time
        expect(calculator.average_round_trip_time).to eq(5)
      end
    end

    context 'with existing average rtt' do
      it 'averages with existing average rtt' do
        calculator.instance_variable_set('@last_round_trip_time', 5)
        calculator.instance_variable_set('@average_round_trip_time', 10)
        calculator.update_average_round_trip_time
        expect(calculator.average_round_trip_time).to eq(9)
      end
    end
  end

  describe '#update_minimum_round_trip_time' do
    context 'with no samples' do
      it 'sets minimum_round_trip_time to zero' do
        calculator.update_minimum_round_trip_time
        expect(calculator.minimum_round_trip_time).to eq(0)
      end
    end

    context 'with one sample' do
      before do
        calculator.instance_variable_set('@last_round_trip_time', 5)
      end

      it 'sets minimum_round_trip_time to zero' do
        calculator.update_minimum_round_trip_time
        expect(calculator.minimum_round_trip_time).to eq(0)
      end
    end

    context 'with two samples' do
      before do
        calculator.instance_variable_set('@last_round_trip_time', 10)
        calculator.instance_variable_set('@rtts', [5])
      end

      it 'sets minimum_round_trip_time to zero' do
        calculator.update_minimum_round_trip_time
        expect(calculator.minimum_round_trip_time).to eq(0)
      end
    end

    context 'with samples less than maximum' do
      before do
        calculator.instance_variable_set('@last_round_trip_time', 10)
        calculator.instance_variable_set('@rtts', [5, 4, 120])
      end

      it 'properly sets minimum_round_trip_time' do
        calculator.update_minimum_round_trip_time
        expect(calculator.minimum_round_trip_time).to eq(4)
      end
    end

    context 'with more than maximum samples' do
      before do
        calculator.instance_variable_set('@last_round_trip_time', 2)
        calculator.instance_variable_set('@rtts', [1, 20, 15, 4, 5, 6, 7, 39, 8, 4])
      end

      it 'properly sets minimum_round_trip_time' do
        calculator.update_minimum_round_trip_time
        expect(calculator.minimum_round_trip_time).to eq(2)
      end
    end

  end

  describe '#measure' do
    context 'block does not raise' do
      it 'updates average rtt' do
        expect(calculator).to receive(:update_average_round_trip_time)
        calculator.measure do
        end
      end

      it 'updates minimum rtt' do
        expect(calculator).to receive(:update_minimum_round_trip_time)
        calculator.measure do
        end
      end
    end

    context 'block raises' do
      it 'does not update average rtt' do
        expect(calculator).not_to receive(:update_average_round_trip_time)
        expect do
          calculator.measure do
            raise "Problem"
          end
        end.to raise_error(/Problem/)
      end

      it 'does not update minimum rtt' do
        expect(calculator).not_to receive(:update_minimum_round_trip_time)
        expect do
          calculator.measure do
            raise "Problem"
          end
        end.to raise_error(/Problem/)
      end
    end
  end
end

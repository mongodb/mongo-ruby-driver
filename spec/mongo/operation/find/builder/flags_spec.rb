# frozen_string_literal: true
# rubocop:todo all

require 'lite_spec_helper'

describe Mongo::Operation::Find::Builder::Flags do

  describe '.map_flags' do

    shared_examples_for 'a flag mapper' do

      let(:flags) do
        described_class.map_flags(options)
      end

      it 'maps allow partial results' do
        expect(flags).to include(:partial)
      end

      it 'maps oplog replay' do
        expect(flags).to include(:oplog_replay)
      end

      it 'maps no cursor timeout' do
        expect(flags).to include(:no_cursor_timeout)
      end

      it 'maps tailable' do
        expect(flags).to include(:tailable_cursor)
      end

      it 'maps await data' do
        expect(flags).to include(:await_data)
      end

      it 'maps exhaust' do
        expect(flags).to include(:exhaust)
      end
    end

    context 'when the options are standard' do

      let(:options) do
        {
          :allow_partial_results => true,
          :oplog_replay => true,
          :no_cursor_timeout => true,
          :tailable => true,
          :await_data => true,
          :exhaust => true
        }
      end

      it_behaves_like 'a flag mapper'
    end

    context 'when the options already have flags' do

      let(:options) do
        {
          :flags => [
            :partial,
            :oplog_replay,
            :no_cursor_timeout,
            :tailable_cursor,
            :await_data,
            :exhaust
          ]
        }
      end

      it_behaves_like 'a flag mapper'
    end

    context 'when the options include tailable_await' do

      let(:options) do
        { :tailable_await => true }
      end

      let(:flags) do
        described_class.map_flags(options)
      end

      it 'maps the await data option' do
        expect(flags).to include(:await_data)
      end

      it 'maps the tailable option' do
        expect(flags).to include(:tailable_cursor)
      end
    end

    context 'when the options provide a cursor type' do

      let(:options) do
        { :cursor_type => :await_data }
      end

      let(:flags) do
        described_class.map_flags(options)
      end

      it 'maps the cursor type to a flag' do
        expect(flags).to include(:await_data)
      end
    end
  end
end

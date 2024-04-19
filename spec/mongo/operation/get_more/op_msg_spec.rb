# frozen_string_literal: true

require 'spec_helper'
require_relative '../shared/csot/examples'

describe Mongo::Operation::GetMore::OpMsg do
  include CSOT::Examples

  let(:spec) do
    {
      options: {},
      db_name: 'db_name',
      coll_name: 'coll_name',
      cursor_id: 1_234_567_890,
    }
  end

  let(:op) { described_class.new(spec) }

  context 'when it is a CSOT-compliant OpMsg' do
    include_examples 'mock CSOT environment'

    context 'when no timeout_ms set' do
      it 'does not set maxTimeMS' do
        expect(body.key?(:maxTimeMS)).to be false
      end
    end

    context 'when timeout_ms is set' do
      let(:remaining_timeout_sec) { 3 }

      context 'when cursor is non-tailable' do
        it 'omits maxTimeMS' do
          expect(body[:maxTimeMS]).to be_nil
        end
      end

      context 'when cursor is tailable' do
        let(:cursor_type) { :tailable }

        it 'omits maxTimeMS' do
          expect(body[:maxTimeMS]).to be_nil
        end
      end

      context 'when cursor is tailable_await' do
        let(:cursor_type) { :tailable_await }

        context 'when max_await_time_ms is omitted' do
          it 'omits maxTimeMS' do
            expect(body[:maxTimeMS]).to be_nil
          end
        end

        context 'when max_await_time_ms is given' do
          let(:max_await_time_ms) { 1_234 }

          it 'sets maxTimeMS' do
            expect(body[:maxTimeMS]).to be == 1_234
          end
        end
      end
    end
  end
end

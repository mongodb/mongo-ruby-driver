# frozen_string_literal: true

require 'spec_helper'
require_relative '../shared/csot/examples'

describe Mongo::Operation::Find::OpMsg do
  include CSOT::Examples

  let(:spec) do
    { coll_name: 'coll_name',
      filter: {},
      db_name: 'db_name' }
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
        let(:cursor_type) { nil }

        context 'when timeout_mode is cursor_lifetime' do
          let(:timeout_mode) { :cursor_lifetime }

          it 'sets maxTimeMS' do
            expect(body[:maxTimeMS]).to be == 3_000
          end
        end

        context 'when timeout_mode is iteration' do
          let(:timeout_mode) { :iteration }

          it 'omits maxTimeMS' do
            expect(body[:maxTimeMS]).to be_nil
          end
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

        it 'sets maxTimeMS' do
          expect(body[:maxTimeMS]).to be == 3_000
        end
      end
    end
  end
end

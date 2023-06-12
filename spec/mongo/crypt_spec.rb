# frozen_string_literal: true

require 'spec_helper'

describe Mongo::Crypt do
  describe '.validate_ffi!' do
    context 'when ffi is available' do
      context 'when ffi is loaded' do
        it 'does not raise' do
          expect do
            described_class.validate_ffi!
          end.not_to raise_error
        end
      end
    end
  end
end

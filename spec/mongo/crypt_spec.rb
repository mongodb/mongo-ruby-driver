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
    # There is no reasonably simple way to test the path where ffi is not
    # available. The ffi gem is a part of our standard test dependencies, so
    # it's always available. So, we would need a dedicated configuration
    # just to test this feature; it seems to be an overhead.
  end
end

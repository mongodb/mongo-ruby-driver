require 'spec_helper'

describe Mongo::Auth::GSSAPI do

  include_context 'authenticatable context'

  it_behaves_like 'an authenticator'

  describe '#initialize' do

    context 'when using JRuby' do

      it 'does not raise an error' do
        expect{auth.db_name}.to_not raise_error
      end
    end

    context 'when not using JRuby' do

      it 'raises an error' do
        expect{auth.db_name}.to raise_error
      end
    end
  end
end

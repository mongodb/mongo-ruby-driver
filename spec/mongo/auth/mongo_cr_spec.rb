require 'spec_helper'

describe Mongo::Auth::MongodbCR do

  include_context 'authenticatable context'

  it_behaves_like 'an authenticator'

  describe '#initialize' do

    context 'when no password is given' do

      it 'raises an error' do

        expect{auth_no_opts.db_name}.to raise_error
      end
    end
  end
end

require 'spec_helper'

describe Mongo::Auth::X509 do

  include_context 'authenticatable context'

  it_behaves_like 'an authenticator'
end

# frozen_string_literal: true
# rubocop:todo all

require 'lite_spec_helper'

describe 'Mongo::Crypt::Binding' do
  require_no_libmongocrypt

  before(:all) do
    if ENV['FLE'] == 'helper'
      skip 'FLE=helper is incompatible with unloaded binding tests'
    end
  end

  context 'when load fails' do

    # JRuby 9.3.2.0 converts our custom LoadErrors to generic NameErrors
    # and trashes the exception messages.
    # https://github.com/jruby/jruby/issues/7070
    # JRuby 9.2 works correctly, this test is skipped on all JRuby versions
    # because we intend to remove JRuby support altogether and therefore
    # adding logic to condition on JRuby versions does not make sense.
    fails_on_jruby

    it 'retries loading at the next reference' do
      lambda do
        Mongo::Crypt::Binding
      end.should raise_error(LoadError, /no path to libmongocrypt specified/)

      # second load should also be attempted and should fail with the
      # LoadError exception
      lambda do
        Mongo::Crypt::Binding
      end.should raise_error(LoadError, /no path to libmongocrypt specified/)
    end
  end
end

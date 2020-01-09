require 'lite_spec_helper'

describe 'Mongo::Crypt::Binding' do
  require_no_libmongocrypt

  context 'when load fails' do

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

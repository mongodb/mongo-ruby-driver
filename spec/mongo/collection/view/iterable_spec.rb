# frozen_string_literal: true
# rubocop:todo all

require 'spec_helper'

describe Mongo::Collection::View::Iterable do
  let(:selector) do
    {}
  end

  let(:options) do
    {}
  end

  let(:view) do
    Mongo::Collection::View.new(authorized_collection, selector, options)
  end

  before do
    authorized_collection.drop
  end

  describe '#each' do
    context 'when allow_disk_use is provided' do
      let(:options) { { allow_disk_use: true } }

      # Other cases are adequately covered by spec tests.
      context 'on server versions < 3.2' do
        max_server_fcv '3.0'

        it 'raises an exception' do
          expect do
            view.each do |document|
              #Do nothing
            end
          end.to raise_error(Mongo::Error::UnsupportedOption, /The MongoDB server handling this request does not support the allow_disk_use option on this command/)
        end
      end
    end
  end
end

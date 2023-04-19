# frozen_string_literal: true
# rubocop:todo all

require 'spec_helper'

describe Mongo::Collection::View::Immutable do

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
    authorized_collection.delete_many
  end

  describe '#configure' do

    context 'when the options have modifiers' do

      let(:options) do
        { :max_time_ms => 500 }
      end

      let(:new_view) do
        view.projection(_id: 1)
      end

      it 'returns a new view' do
        expect(view).not_to be(new_view)
      end

      it 'creates a new options hash' do
        expect(view.options).not_to be(new_view.options)
      end

      it 'keeps the modifier fields already in the options hash' do
        expect(new_view.modifiers[:$maxTimeMS]).to eq(500)
      end

      it 'sets the option' do
        expect(new_view.projection).to eq('_id' => 1)
      end

      it 'creates a new modifiers document' do
        expect(view.modifiers).not_to be(new_view.modifiers)
      end
    end
  end
end

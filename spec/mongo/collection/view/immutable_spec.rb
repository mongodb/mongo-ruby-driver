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

  after do
    authorized_collection.delete_many
  end

  describe '#configure' do

    context 'when the options has a modifiers document' do

      let(:options) do
        { modifiers: { :$maxTimeMS => 500 } }
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
        expect(new_view.projection).to eq(_id: 1)
      end

      it 'creates a new modifiers document' do
        expect(view.modifiers).not_to be(new_view.modifiers)
      end
    end
  end

  describe '#configure_modifier' do

    let(:new_view) do
      view.sort('x' => Mongo::Index::ASCENDING)
    end

    context 'when the options does not have a modifiers document' do

      it 'returns a new view' do
        expect(view).not_to be(new_view)
      end

      it 'returns a new view with the modifiers document containing the option' do
        expect(new_view.modifiers[:$orderby]).to eq({ 'x' => Mongo::Index::ASCENDING })
      end
    end

    context 'when the options has a modifiers document' do

      let(:options) do
        { modifiers: { :$maxTimeMS => 500 } }
      end

      it 'returns a new view' do
        expect(view).not_to be(new_view)
      end

      it 'creates a new options hash' do
        expect(view.options).not_to be(new_view.options)
      end

      it 'keeps the fields already in the options hash and merges in the new one' do
        expect(new_view.modifiers[:$maxTimeMS]).to eq(500)
      end

      it 'sets the new value in the new view modifier document' do
        expect(new_view.modifiers[:$orderby]).to eq('x' => Mongo::Index::ASCENDING)
      end

      it 'returns that value when the corresponding option method is called' do
        expect(new_view.sort).to eq({ 'x' => Mongo::Index::ASCENDING })
      end

      it 'creates a new modifiers document' do
        expect(view.modifiers).not_to be(new_view.modifiers)
      end
    end
  end
end

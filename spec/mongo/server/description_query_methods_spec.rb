# frozen_string_literal: true
# rubocop:todo all

require 'spec_helper'

# For conciseness these tests are arranged by description types
# rather than by methods being tested, as is customary
describe Mongo::Server::Description do
  let(:address) do
    Mongo::Address.new(authorized_primary.address.to_s)
  end

  let(:desc_options) { {} }
  let(:ok) { 1 }
  let(:description) { described_class.new(address, desc_options) }

  shared_examples_for 'is unknown' do
    it 'is unknown' do
      expect(description).to be_unknown
    end

    %w(
      arbiter ghost hidden mongos passive primary secondary standalone
      other
    ).each do |type|
      it "is not #{type}" do
        expect(description.send("#{type}?")).to be false
      end
    end

    it 'is not data-bearing' do
      expect(description.data_bearing?).to be false
    end
  end

  context 'unknown' do
    context 'empty description' do
      it_behaves_like 'is unknown'
    end
  end

  context 'ghost' do
    let(:desc_options) { {'isreplicaset' => true,
      'minWireVersion' => 2, 'maxWireVersion' => 8, 'ok' => ok} }

    it 'is ghost' do
      expect(description).to be_ghost
    end

    %w(
      arbiter hidden mongos passive primary secondary standalone
      other unknown
    ).each do |type|
      it "is not #{type}" do
        expect(description.send("#{type}?")).to be false
      end
    end

    it 'is not data-bearing' do
      expect(description.data_bearing?).to be false
    end

    context 'ok: 0' do
      let(:ok) { 0 }

      it_behaves_like 'is unknown'
    end
  end

  context 'mongos' do
    let(:desc_options) { {'msg' => 'isdbgrid',
      'minWireVersion' => 2, 'maxWireVersion' => 8, 'ok' => ok} }

    it 'is mongos' do
      expect(description).to be_mongos
    end

    %w(
      arbiter hidden passive primary secondary standalone
      other unknown ghost
    ).each do |type|
      it "is not #{type}" do
        expect(description.send("#{type}?")).to be false
      end
    end

    it 'is data-bearing' do
      expect(description.data_bearing?).to be true
    end

    context 'ok: 0' do
      let(:ok) { 0 }

      it_behaves_like 'is unknown'
    end
  end

  context 'primary' do
    let(:desc_options) { {'isWritablePrimary' => true,
      'minWireVersion' => 2, 'maxWireVersion' => 8,
      'setName' => 'foo', 'ok' => ok} }

    it 'is primary' do
      expect(description).to be_primary
    end

    %w(
      arbiter hidden passive mongos secondary standalone
      other unknown ghost
    ).each do |type|
      it "is not #{type}" do
        expect(description.send("#{type}?")).to be false
      end
    end

    it 'is data-bearing' do
      expect(description.data_bearing?).to be true
    end

    context 'ok: 0' do
      let(:ok) { 0 }

      it_behaves_like 'is unknown'
    end
  end

  context 'secondary' do
    let(:desc_options) { {'secondary' => true,
      'minWireVersion' => 2, 'maxWireVersion' => 8,
      'setName' => 'foo', 'ok' => ok} }

    it 'is secondary' do
      expect(description).to be_secondary
    end

    %w(
      arbiter hidden passive mongos primary standalone
      other unknown ghost
    ).each do |type|
      it "is not #{type}" do
        expect(description.send("#{type}?")).to be false
      end
    end

    it 'is data-bearing' do
      expect(description.data_bearing?).to be true
    end

    context 'ok: 0' do
      let(:ok) { 0 }

      it_behaves_like 'is unknown'
    end

    it 'is not passive' do
      expect(description).not_to be_passive
    end

    context 'passive' do
      let(:desc_options) { {'secondary' => true,
        'minWireVersion' => 2, 'maxWireVersion' => 8,
        'setName' => 'foo', 'passive' => true, 'ok' => ok} }

      it 'is passive' do
        expect(description).to be_passive
      end

      it 'is data-bearing' do
        expect(description.data_bearing?).to be true
      end

      context 'ok: 0' do
        let(:ok) { 0 }

        it_behaves_like 'is unknown'

        it 'is not passive' do
          expect(description).not_to be_passive
        end
      end
    end
  end

  context 'arbiter' do
    let(:desc_options) { {'arbiterOnly' => true,
      'minWireVersion' => 2, 'maxWireVersion' => 8,
      'setName' => 'foo', 'ok' => ok} }

    it 'is arbiter' do
      expect(description).to be_arbiter
    end

    %w(
      secondary hidden passive mongos primary standalone
      other unknown ghost
    ).each do |type|
      it "is not #{type}" do
        expect(description.send("#{type}?")).to be false
      end
    end

    it 'is not data-bearing' do
      expect(description.data_bearing?).to be false
    end

    context 'ok: 0' do
      let(:ok) { 0 }

      it_behaves_like 'is unknown'
    end
  end

  context 'standalone' do
    let(:desc_options) { {'minWireVersion' => 2, 'maxWireVersion' => 8, 'ok' => ok} }

    it 'is standalone' do
      expect(description).to be_standalone
    end

    %w(
      secondary hidden passive mongos primary arbiter
      other unknown ghost
    ).each do |type|
      it "is not #{type}" do
        expect(description.send("#{type}?")).to be false
      end
    end

    it 'is data-bearing' do
      expect(description.data_bearing?).to be true
    end

    context 'ok: 0' do
      let(:ok) { 0 }

      it_behaves_like 'is unknown'
    end
  end

  context 'other' do

    shared_examples_for 'is other' do

      it 'is other' do
        expect(description).to be_other
      end

      %w(
        secondary passive mongos primary arbiter
        standalone unknown ghost
      ).each do |type|
        it "is not #{type}" do
          expect(description.send("#{type}?")).to be false
        end
      end

      it 'is not data-bearing' do
        expect(description.data_bearing?).to be false
      end

      context 'ok: 0' do
        let(:ok) { 0 }

        it_behaves_like 'is unknown'
      end
    end

    context 'hidden: true' do
      let(:desc_options) { {'setName' => 'foo',
        'minWireVersion' => 2, 'maxWireVersion' => 8,
        'hidden' => true, 'ok' => ok} }

      it_behaves_like 'is other'

      it 'is hidden' do
        expect(description).to be_hidden
      end
    end

    context 'not hidden: true' do
      let(:desc_options) { {'setName' => 'foo',
        'minWireVersion' => 2, 'maxWireVersion' => 8, 'ok' => ok} }

      it_behaves_like 'is other'

      it 'is not hidden' do
        expect(description).not_to be_hidden
      end
    end
  end
end

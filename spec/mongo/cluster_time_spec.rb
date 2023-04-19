# frozen_string_literal: true
# rubocop:todo all

require 'lite_spec_helper'

describe Mongo::ClusterTime do
  describe '#>=' do
    context 'equal but different objects' do
      let(:one) { described_class.new(clusterTime: BSON::Timestamp.new(123, 456)) }
      let(:two) { described_class.new(clusterTime: BSON::Timestamp.new(123, 456)) }

      it 'is true' do
        expect(one).to be >= two
      end
    end

    context 'first is greater' do
      let(:one) { described_class.new(clusterTime: BSON::Timestamp.new(124, 456)) }
      let(:two) { described_class.new(clusterTime: BSON::Timestamp.new(123, 456)) }

      it 'is true' do
        expect(one).to be >= two
      end
    end

    context 'second is greater' do
      let(:one) { described_class.new(clusterTime: BSON::Timestamp.new(123, 456)) }
      let(:two) { described_class.new(clusterTime: BSON::Timestamp.new(123, 457)) }

      it 'is false' do
        expect(one).not_to be >= two
      end
    end
  end

  describe '#>' do
    context 'equal but different objects' do
      let(:one) { described_class.new(clusterTime: BSON::Timestamp.new(123, 456)) }
      let(:two) { described_class.new(clusterTime: BSON::Timestamp.new(123, 456)) }

      it 'is false' do
        expect(one).not_to be > two
      end
    end

    context 'first is greater' do
      let(:one) { described_class.new(clusterTime: BSON::Timestamp.new(124, 456)) }
      let(:two) { described_class.new(clusterTime: BSON::Timestamp.new(123, 456)) }

      it 'is true' do
        expect(one).to be > two
      end
    end

    context 'second is greater' do
      let(:one) { described_class.new(clusterTime: BSON::Timestamp.new(123, 456)) }
      let(:two) { described_class.new(clusterTime: BSON::Timestamp.new(123, 457)) }

      it 'is false' do
        expect(one).not_to be > two
      end
    end
  end

  describe '#<=' do
    context 'equal but different objects' do
      let(:one) { described_class.new(clusterTime: BSON::Timestamp.new(123, 456)) }
      let(:two) { described_class.new(clusterTime: BSON::Timestamp.new(123, 456)) }

      it 'is true' do
        expect(one).to be <= two
      end
    end

    context 'first is greater' do
      let(:one) { described_class.new(clusterTime: BSON::Timestamp.new(124, 456)) }
      let(:two) { described_class.new(clusterTime: BSON::Timestamp.new(123, 456)) }

      it 'is false' do
        expect(one).not_to be <= two
      end
    end

    context 'second is greater' do
      let(:one) { described_class.new(clusterTime: BSON::Timestamp.new(123, 456)) }
      let(:two) { described_class.new(clusterTime: BSON::Timestamp.new(123, 457)) }

      it 'is true' do
        expect(one).to be <= two
      end
    end
  end

  describe '#<' do
    context 'equal but different objects' do
      let(:one) { described_class.new(clusterTime: BSON::Timestamp.new(123, 456)) }
      let(:two) { described_class.new(clusterTime: BSON::Timestamp.new(123, 456)) }

      it 'is false' do
        expect(one).not_to be < two
      end
    end

    context 'first is greater' do
      let(:one) { described_class.new(clusterTime: BSON::Timestamp.new(124, 456)) }
      let(:two) { described_class.new(clusterTime: BSON::Timestamp.new(123, 456)) }

      it 'is false' do
        expect(one).not_to be < two
      end
    end

    context 'second is greater' do
      let(:one) { described_class.new(clusterTime: BSON::Timestamp.new(123, 456)) }
      let(:two) { described_class.new(clusterTime: BSON::Timestamp.new(123, 457)) }

      it 'is true' do
        expect(one).to be < two
      end
    end
  end

  describe '#==' do
    context 'equal but different objects' do
      let(:one) { described_class.new(clusterTime: BSON::Timestamp.new(123, 456)) }
      let(:two) { described_class.new(clusterTime: BSON::Timestamp.new(123, 456)) }

      it 'is true' do
        expect(one).to be == two
      end
    end

    context 'first is greater' do
      let(:one) { described_class.new(clusterTime: BSON::Timestamp.new(124, 456)) }
      let(:two) { described_class.new(clusterTime: BSON::Timestamp.new(123, 456)) }

      it 'is false' do
        expect(one).not_to be == two
      end
    end

    context 'second is greater' do
      let(:one) { described_class.new(clusterTime: BSON::Timestamp.new(123, 456)) }
      let(:two) { described_class.new(clusterTime: BSON::Timestamp.new(123, 457)) }

      it 'is false' do
        expect(one).not_to be == two
      end
    end
  end
end

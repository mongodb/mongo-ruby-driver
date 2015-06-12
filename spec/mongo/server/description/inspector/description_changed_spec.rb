require 'spec_helper'

describe Mongo::Server::Description::Inspector::DescriptionChanged do

  let(:listeners) do
    Mongo::Event::Listeners.new
  end

  let(:inspection) do
    described_class.new(listeners)
  end

  let(:address) do
    Mongo::Address.new('127.0.0.1:27017')
  end

  describe '.run' do

    let(:config) do
      {
          'ismaster' => true,
          'secondary' => false,
          'hosts' => [ '127.0.0.1:27018', '127.0.0.1:27019' ],
          'setName' => 'test'
      }
    end

    let(:description) do
      Mongo::Server::Description.new(address, config, listeners)
    end

    let(:updated) do
      Mongo::Server::Description.new(address, new_config, listeners)
    end

    let(:listener) do
      double('listener')
    end

    before do
      listeners.add_listener(Mongo::Event::DESCRIPTION_CHANGED, listener)
    end

    context 'when there is no change' do

      let(:new_config) do
        {
            'ismaster' => true,
            'secondary' => false,
            'hosts' => [ '127.0.0.1:27018', '127.0.0.1:27019' ],
            'setName' => 'test'
        }
      end

      it 'does not fire a description changed event' do
        expect(listener).to_not receive(:handle)
        inspection.run(description, updated)
      end
    end

    context 'when there is a change' do

      let(:new_config) do
        {
            'ismaster' => true,
            'secondary' => false,
            'hosts' => [ '127.0.0.1:27018', '127.0.0.1:27020' ],
            'setName' => 'test'
        }
      end

      it 'fires a description changed event' do
        expect(listener).to receive(:handle)
        inspection.run(description, updated)
      end
    end
  end
end

require 'spec_helper'

describe Mongo::Monitoring do

  describe '#dup' do

    let(:monitoring) do
      described_class.new
    end

    let(:copy) do
      monitoring.dup
    end

    it 'dups the subscribers' do
      expect(monitoring.subscribers).to_not equal(copy.subscribers)
    end

    it 'keeps the same subscriber instances' do
      expect(monitoring.subscribers).to eq(copy.subscribers)
    end

    context 'when adding to the copy' do

      let(:subscriber) do
        double('subscriber')
      end

      before do
        copy.subscribe('topic', subscriber)
      end

      it 'does not modify the original subscribers' do
        expect(monitoring.subscribers).to_not eq(copy.subscribers)
      end
    end
  end

  describe '#initialize' do
    let(:custom_subscriber) { Object.new }

    before do
      Mongo::Monitoring::Global.subscribe(Mongo::Monitoring::COMMAND, custom_subscriber)
    end

    after do
      Mongo::Monitoring::Global.unsubscribe(Mongo::Monitoring::COMMAND, custom_subscriber)
    end

    shared_examples 'includes the global subscribers' do

      it 'subscribes the CommandLogSubscriber to the COMMAND topic' do
        expect(monitoring.subscribers[Mongo::Monitoring::COMMAND]).to include(custom_subscriber)
      end
    end

    shared_examples 'includes the builtin subscribers' do

      it 'subscribes the CommandLogSubscriber to the COMMAND topic' do
        expect(monitoring.subscribers[Mongo::Monitoring::COMMAND]).to include(an_instance_of(Mongo::Monitoring::CommandLogSubscriber))
      end

      it 'does not subscribe the ServerOpeningLogSubscriber to the SERVER_OPENING topic' do
        expect(monitoring.subscribers[Mongo::Monitoring::SERVER_OPENING]).to include(an_instance_of(Mongo::Monitoring::ServerOpeningLogSubscriber))
      end

      it 'does not subscribe the ServerClosedLogSubscriber to the SERVER_CLOSED topic' do
        expect(monitoring.subscribers[Mongo::Monitoring::SERVER_CLOSED]).to include(an_instance_of(Mongo::Monitoring::ServerClosedLogSubscriber))
      end

      it 'does not subscribe the ServerDescriptionChangedLogSubscriber to the SERVER_DESCRIPTION_CHANGED topic' do
        expect(monitoring.subscribers[Mongo::Monitoring::SERVER_DESCRIPTION_CHANGED]).to include(an_instance_of(Mongo::Monitoring::ServerDescriptionChangedLogSubscriber))
      end

      it 'does not subscribe the TopologyOpeningLogSubscriber to the TOPOLOGY_OPENING topic' do
        expect(monitoring.subscribers[Mongo::Monitoring::TOPOLOGY_OPENING]).to include(an_instance_of(Mongo::Monitoring::TopologyOpeningLogSubscriber))
      end

      it 'does not subscribe the TopologyChangedLogSubscriber to the TOPOLOGY_CHANGED topic' do
        expect(monitoring.subscribers[Mongo::Monitoring::TOPOLOGY_CHANGED]).to include(an_instance_of(Mongo::Monitoring::TopologyChangedLogSubscriber))
      end

      it 'does not subscribe the TopologyClosedLogSubscriber to the TOPOLOGY_CLOSED topic' do
        expect(monitoring.subscribers[Mongo::Monitoring::TOPOLOGY_CLOSED]).to include(an_instance_of(Mongo::Monitoring::TopologyClosedLogSubscriber))
      end
    end

    shared_examples 'does not subscribe the CmapLogSubscriber' do

      it 'does not subscribe the CmapLogSubscriber to the CONNECTION_POOL topic' do
        expect(monitoring.subscribers[Mongo::Monitoring::CONNECTION_POOL] || []).to_not include(an_instance_of(Mongo::Monitoring::CmapLogSubscriber))
      end
    end

    context 'when no monitoring options provided' do

      let(:monitoring) do
        described_class.new
      end

      it_behaves_like 'includes the builtin subscribers'

      it_behaves_like 'does not subscribe the CmapLogSubscriber'

      it_behaves_like 'includes the global subscribers'
    end

    context 'when monitoring options provided' do

      context 'when monitoring is true' do

        let(:monitoring) do
          described_class.new(monitoring: true)
        end

        it_behaves_like 'includes the builtin subscribers'

        it_behaves_like 'does not subscribe the CmapLogSubscriber'

        it_behaves_like 'includes the global subscribers'
      end

      context 'when monitoring is false' do

        let(:monitoring) do
          described_class.new(monitoring: false)
        end

        it 'does not include any subscribers' do
          expect(monitoring.subscribers.values).to be_empty
        end
      end

      context 'when monitoring is an empty Hash' do

        let(:monitoring) do
          described_class.new(monitoring: {})
        end

        it_behaves_like 'includes the builtin subscribers'

        it_behaves_like 'does not subscribe the CmapLogSubscriber'

        it_behaves_like 'includes the global subscribers'
      end

      context 'when monitoring is a Hash with key "builtins"' do

        context 'and value "true"' do
          let(:monitoring) do
            described_class.new(monitoring: { builtins: true })
          end

          it_behaves_like 'includes the builtin subscribers'

          it_behaves_like 'does not subscribe the CmapLogSubscriber'

          it_behaves_like 'includes the global subscribers'
        end

        context 'and value "false"' do
          let(:monitoring) do
            described_class.new(monitoring: { builtins: false })
          end

          it 'does not include the builtin subscribers' do
            expect(monitoring.subscribers.values.flatten).to match_array([custom_subscriber])
          end

          it_behaves_like 'does not subscribe the CmapLogSubscriber'

          it_behaves_like 'includes the global subscribers'
        end

        context 'and value "true"' do
          let(:monitoring) do
            described_class.new(monitoring: { builtins: { connection_pool: true } })
          end

          it_behaves_like 'includes the builtin subscribers'

          it 'subscribes the CmapLogSubscriber to the CONNECTION_POOL topic' do
            expect(monitoring.subscribers[Mongo::Monitoring::CONNECTION_POOL]).to include(an_instance_of(Mongo::Monitoring::CmapLogSubscriber))
          end

          it_behaves_like 'includes the global subscribers'
        end
      end
    end
  end

  describe '#subscribe' do

    let(:monitoring) do
      described_class.new(monitoring: false)
    end

    let(:subscriber) do
      double('subscriber')
    end

    it 'subscribes to the topic' do
      monitoring.subscribe('topic', subscriber)
      expect(monitoring.subscribers['topic']).to eq([ subscriber ])
    end

    it 'subscribes to the topic twice' do
      monitoring.subscribe('topic', subscriber)
      monitoring.subscribe('topic', subscriber)
      expect(monitoring.subscribers['topic']).to eq([ subscriber, subscriber ])
    end
  end

  describe '#unsubscribe' do

    let(:monitoring) do
      described_class.new(monitoring: false)
    end

    let(:subscriber) do
      double('subscriber')
    end

    it 'unsubscribes from the topic' do
      monitoring.subscribe('topic', subscriber)
      monitoring.unsubscribe('topic', subscriber)
      expect(monitoring.subscribers['topic']).to eq([ ])
    end

    it 'unsubscribes from the topic when not subscribed' do
      monitoring.unsubscribe('topic', subscriber)
      expect(monitoring.subscribers['topic']).to eq([ ])
    end
  end

  describe '#started' do

    let(:monitoring) do
      described_class.new(monitoring: false)
    end

    let(:subscriber) do
      double('subscriber')
    end

    let(:event) do
      double('event')
    end

    before do
      monitoring.subscribe('topic', subscriber)
    end

    it 'calls the started method on each subscriber' do
      expect(subscriber).to receive(:started).with(event)
      monitoring.started('topic', event)
    end
  end

  describe '#succeeded' do

    let(:monitoring) do
      described_class.new(monitoring: false)
    end

    let(:subscriber) do
      double('subscriber')
    end

    let(:event) do
      double('event')
    end

    before do
      monitoring.subscribe('topic', subscriber)
    end

    it 'calls the succeeded method on each subscriber' do
      expect(subscriber).to receive(:succeeded).with(event)
      monitoring.succeeded('topic', event)
    end
  end

  describe '#failed' do

    let(:monitoring) do
      described_class.new(monitoring: false)
    end

    let(:subscriber) do
      double('subscriber')
    end

    let(:event) do
      double('event')
    end

    before do
      monitoring.subscribe('topic', subscriber)
    end

    it 'calls the failed method on each subscriber' do
      expect(subscriber).to receive(:failed).with(event)
      monitoring.failed('topic', event)
    end
  end
end

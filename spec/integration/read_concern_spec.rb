# frozen_string_literal: true
# rubocop:todo all

require 'spec_helper'

describe 'read concern' do
  min_server_version '3.2'

  let(:subscriber) do
    Mrss::EventSubscriber.new
  end

  let(:specified_read_concern) do
    { :level => :local }
  end

  let(:expected_read_concern) do
    { 'level' => 'local' }
  end

  let(:sent_read_concern) do
    subscriber.clear_events!
    collection.count_documents
    subscriber.started_events.find { |c| c.command_name == 'aggregate' }.command[:readConcern]
  end

  shared_examples_for 'a read concern is specified' do
    it 'sends a read concern to the server' do
      expect(sent_read_concern).to eq(expected_read_concern)
    end
  end

  shared_examples_for 'no read concern is specified' do
    it 'does not send a read concern to the server' do
      expect(sent_read_concern).to be_nil
    end
  end

  context 'when the client has no read concern specified' do

    let(:client) do
      authorized_client.tap do |client|
        client.subscribe(Mongo::Monitoring::COMMAND, subscriber)
      end
    end

    context 'when the collection has no read concern specified' do

      let(:collection) do
        client[TEST_COLL]
      end

      it_behaves_like 'no read concern is specified'
    end

    context 'when the collection has a read concern specified' do

      let(:collection) do
        client[TEST_COLL].with(read_concern: specified_read_concern)
      end

      it_behaves_like 'a read concern is specified'
    end
  end

  context 'when the client has a read concern specified' do

    let(:client) do
      authorized_client.with(read_concern: specified_read_concern).tap do |client|
        client.subscribe(Mongo::Monitoring::COMMAND, subscriber)
      end
    end

    context 'when the collection has no read concern specified' do

      let(:collection) do
        client[TEST_COLL]
      end

      it_behaves_like 'a read concern is specified'
    end

    context 'when the collection has a read concern specified' do

      let(:collection) do
        client[TEST_COLL].with(read_concern: specified_read_concern)
      end

      it_behaves_like 'a read concern is specified'
    end
  end
end

require 'spec_helper'

describe 'getMore operation' do
  # https://jira.mongodb.org/browse/RUBY-1987
  min_server_fcv '3.2'

  let(:collection) do
    subscribed_client['get_more_spec']
  end

  let(:scope) do
    collection.find.batch_size(1).each
  end

  before do
    collection.delete_many
    collection.insert_one(a: 1)
    #collection.insert_one(a: 2)
    EventSubscriber.clear_events!
  end

  let(:get_more_command) do
    event = EventSubscriber.single_command_started_event('getMore')
    event.command['getMore']
  end

  it 'sends cursor id as int64' do
    scope.to_a

    expect(get_more_command).to be_a(BSON::Int64)
  end
end

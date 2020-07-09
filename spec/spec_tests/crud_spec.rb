require 'spec_helper'

require 'runners/crud'

describe 'CRUD v1 spec tests' do
  define_crud_spec_tests(CRUD_TESTS) do |spec, req, test|
    let(:client) { authorized_client }
  end
end

describe 'CRUD v2 spec tests' do
  define_crud_spec_tests(CRUD2_TESTS) do |spec, req, test|
    let(:client) do
      authorized_client.use(spec.database_name || 'crud-default').tap do |client|
        client.subscribe(Mongo::Monitoring::COMMAND, event_subscriber)
      end
    end
  end
end

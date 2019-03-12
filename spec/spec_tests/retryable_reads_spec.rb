require 'spec_helper'

describe do
  define_crud_spec_tests('Retryable reads spec tests', RETRYABLE_READS_TESTS.sort) do |spec, req, test|
    let(:client) do
      root_authorized_client.with({max_read_retries: 0}.update(test.client_options))
    end

    let(:collection) { client['crud_spec_test'] }

    before do
      if req.nil? || req.satisfied?
        collection.delete_many
        test.setup_test(collection)
      end
    end

    after do
      if req.nil? || req.satisfied?
        test.clear_fail_point(client)
      end
    end
  end
end

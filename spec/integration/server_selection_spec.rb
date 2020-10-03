require 'spec_helper'

describe 'Server selection' do
  context 'replica set' do
    require_topology :replica_set

    context 'when mixed case tag names are used' do
      # For simplicity this test assumes our Evergreen configuration:
      # nodes are started from port 27017 onwards and there are more than
      # one of them.

      let(:desired_index) do
        if authorized_client.cluster.next_primary.address.port == 27017
          1
        else
          0
        end
      end

      let(:client) do
        new_local_client(SpecConfig.instance.addresses,
          SpecConfig.instance.test_options.merge(
            server_selection_timeout: 4,
            read: {mode: :secondary, tag_sets: ['nodeIndex' => desired_index]},
          ))
      end

      it 'selects the server' do
        client['nonexistent'].count.should == 0
      end
    end
  end
end

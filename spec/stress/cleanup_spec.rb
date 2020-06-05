require 'spec_helper'

describe 'Cleanup stress test' do
  require_stress

  let(:options) do
    SpecConfig.instance.all_test_options
  end

  before(:all) do
    # load if necessary
    ClusterConfig.instance.primary_address
    ClientRegistry.instance.close_all_clients
  end

  context 'single client disconnect/reconnect' do
    let(:client) do
      new_local_client([ClusterConfig.instance.primary_address.seed], options)
    end

    it 'cleans up' do
      client

      start_resources = resources

      100.times do
        client.close
        client.reconnect
      end

      end_resources = resources

      end_resources.should == start_resources
    end
  end

  def resources
    {
      open_file_count: Dir["/proc/#{Process.pid}/fd/*"].count,
      running_thread_count: Thread.list.select { |thread| thread.status == 'run' }.count,
    }
  end
end

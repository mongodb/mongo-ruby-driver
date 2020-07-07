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

      500.times do
        client.close
        client.reconnect
      end

      end_resources = resources

      # There seem to be a temporary file descriptor leak in CI,
      # where we start with 75 fds and end with 77 fds.
      # Allow a few to be leaked, run more iterations to ensure the leak
      # is not a real one.
      end_resources[:open_file_count].should >= start_resources[:open_file_count]
      end_resources[:open_file_count].should <= start_resources[:open_file_count] + 5

      end_resources[:running_thread_count].should == start_resources[:running_thread_count]
    end
  end

  def resources
    {
      open_file_count: Dir["/proc/#{Process.pid}/fd/*"].count,
      running_thread_count: Thread.list.select { |thread| thread.status == 'run' }.count,
    }
  end
end

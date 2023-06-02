# frozen_string_literal: true
# rubocop:todo all

module CommonShortcuts
  module ClassMethods
    # Declares a topology double, which is configured to accept summary
    # calls as those are used in SDAM event creation
    def declare_topology_double
      let(:topology) do
        double('topology').tap do |topology|
          allow(topology).to receive(:summary)
        end
      end
    end

    # For tests which require clients to connect, clean slate asks all
    # existing clients to be closed prior to the test execution.
    # Note that clean_slate closes all clients for each test in the scope.
    def clean_slate
      before do
        ClientRegistry.instance.close_all_clients
        BackgroundThreadRegistry.instance.verify_empty!
      end
    end

    # Similar to clean slate but closes clients once before all tests in
    # the scope. Use when the tests do not create new clients but do not
    # want any background output from previously existing clients.
    def clean_slate_for_all
      before(:all) do
        ClientRegistry.instance.close_all_clients
        BackgroundThreadRegistry.instance.verify_empty!
      end
    end

    # If only the lite spec helper was loaded, this method does nothing.
    # If the full spec helper was loaded, this method performs the same function
    # as clean_state_for_all.
    def clean_slate_for_all_if_possible
      before(:all) do
        if defined?(ClusterTools)
          ClientRegistry.instance.close_all_clients
          BackgroundThreadRegistry.instance.verify_empty!
        end
      end
    end

    # For some reason, there are tests which fail on evergreen either
    # intermittently or reliably that always succeed locally.
    # Debugging of tests in evergreen is difficult/impossible,
    # thus this workaround.
    def clean_slate_on_evergreen
      before(:all) do
        if SpecConfig.instance.ci?
          ClientRegistry.instance.close_all_clients
        end
      end
    end

    # Applies environment variable overrides in +env+ to the global environment
    # (+ENV+) for the duration of each test.
    #
    # If a key's value in +env+ is nil, this key is removed from +ENV+.
    #
    # When the test finishes, the values in original +ENV+ that were overridden
    # by +env+ are restored. If a key was not in original +ENV+ and was
    # overridden by +env+, this key is removed from +ENV+ after the test.
    #
    # If the environment variables are not known at test definition time
    # but are determined at test execution time, pass a block instead of
    # the +env+ parameter and return the desired environment variables as
    # a Hash from the block.
    def local_env(env = nil, &block)
      around do |example|
        env ||= block.call

        # This duplicates ENV.
        # Note that ENV.dup produces an Object which does not behave like
        # the original ENV, and hence is not usable.
        saved_env = ENV.to_h
        env.each do |k, v|
          if v.nil?
            ENV.delete(k)
          else
            ENV[k] = v
          end
        end

        begin
          example.run
        ensure
          env.each do |k, v|
            if saved_env.key?(k)
              ENV[k] = saved_env[k]
            else
              ENV.delete(k)
            end
          end
        end
      end
    end

    def clear_ocsp_cache
      before do
        Mongo.clear_ocsp_cache
      end
    end

    def with_ocsp_mock(ca_file_path, responder_cert_path, responder_key_path,
      fault: nil, port: 8100
    )
      clear_ocsp_cache

      around do |example|
        args = [
          SpecConfig.instance.ocsp_files_dir.join('ocsp_mock.py').to_s,
          '--ca_file', ca_file_path.to_s,
          '--ocsp_responder_cert', responder_cert_path.to_s,
          '--ocsp_responder_key', responder_key_path.to_s,
          '-p', port.to_s,
        ]
        if SpecConfig.instance.client_debug?
          # Use when debugging - tests run faster without -v.
          args << '-v'
        end
        if fault
          args += ['--fault', fault]
        end
        process = ChildProcess.new(*args)

        process.io.inherit!

        retried = false
        begin
          process.start
        rescue
          if retried
            raise
          else
            sleep 1
            retried = true
            retry
          end
        end

        begin
          sleep 0.4
          example.run
        ensure
          if process.exited?
            raise "Spawned process exited before we stopped it"
          end

          process.stop
          process.wait
        end
      end
    end

    def with_openssl_debug
      around do |example|
        v = OpenSSL.debug
        OpenSSL.debug = true
        begin
          example.run
        ensure
          OpenSSL.debug = v
        end
      end
    end
  end

  module InstanceMethods
    def kill_all_server_sessions
      begin
        ClientRegistry.instance.global_client('root_authorized').command(killAllSessions: [])
      # killAllSessions also kills the implicit session which the driver uses
      # to send this command, as a result it always fails
      rescue Mongo::Error::OperationFailure => e
        # "operation was interrupted"
        unless e.code == 11601
          raise
        end
      end
    end

    def wait_for_all_servers(cluster)
      # Cluster waits for initial round of sdam until the primary
      # is discovered, which means by the time a connection is obtained
      # here some of the servers in the topology may still be unknown.
      # This messes with event expectations below. Therefore, wait for
      # all servers in the topology to be checked.
      #
      # This wait here assumes all addresses specified for the test
      # suite are for working servers of the cluster; if this is not
      # the case, this test will fail due to exceeding the general
      # test timeout eventually.
      while cluster.servers_list.any? { |server| server.unknown? }
        warn "Waiting for unknown servers in #{cluster.summary}"
        sleep 0.25
      end
    end

    def make_server(mode, options = {})
      tags = options[:tags] || {}
      average_round_trip_time = if mode == :unknown
        nil
      else
        options[:average_round_trip_time] || 0
      end

      if mode == :unknown
        config = {}
      else
        config = {
          'isWritablePrimary' => mode == :primary,
          'secondary' => mode == :secondary,
          'arbiterOnly' => mode == :arbiter,
          'isreplicaset' => mode == :ghost,
          'hidden' => mode == :other,
          'msg' => mode == :mongos ? 'isdbgrid' : nil,
          'tags' => tags,
          'ok' => 1,
          'minWireVersion' => 2, 'maxWireVersion' => 8,
        }
        if [:primary, :secondary, :arbiter, :other].include?(mode)
          config['setName'] = 'mongodb_set'
        end
      end

      listeners = Mongo::Event::Listeners.new
      monitoring = Mongo::Monitoring.new
      address = options[:address]

      cluster = double('cluster')
      allow(cluster).to receive(:topology).and_return(topology)
      allow(cluster).to receive(:app_metadata)
      allow(cluster).to receive(:options).and_return({})
      allow(cluster).to receive(:run_sdam_flow)
      allow(cluster).to receive(:monitor_app_metadata)
      allow(cluster).to receive(:push_monitor_app_metadata)
      allow(cluster).to receive(:heartbeat_interval).and_return(10)
      server = Mongo::Server.new(address, cluster, monitoring, listeners,
        monitoring_io: false)
      # Since the server references a double for the cluster, the server
      # must be closed in the scope of the example.
      register_server(server)
      description = Mongo::Server::Description.new(
        address, config,
        average_round_trip_time: average_round_trip_time,
      )
      server.tap do |s|
        allow(s).to receive(:description).and_return(description)
      end
    end

    def make_protocol_reply(payload)
      Mongo::Protocol::Reply.new.tap do |reply|
        reply.instance_variable_set('@flags', [])
        reply.instance_variable_set('@documents', [payload])
      end
    end

    def make_not_master_reply
      make_protocol_reply(
        'ok' => 0, 'code' => 10107, 'errmsg' => 'not master'
      )
    end

    def make_node_recovering_reply
      make_protocol_reply(
        'ok' => 0, 'code' => 11602, 'errmsg' => 'InterruptedDueToStepDown'
      )
    end

    def make_node_shutting_down_reply
      make_protocol_reply(
        'ok' => 0, 'code' => 91, 'errmsg' => 'shutdown in progress'
      )
    end

    def register_cluster(cluster)
      finalizer = lambda do |cluster|
        cluster.close
      end
      LocalResourceRegistry.instance.register(cluster, finalizer)
    end

    def register_server(server)
      finalizer = lambda do |server|
        if server.connected?
          server.close
        end
      end
      LocalResourceRegistry.instance.register(server, finalizer)
    end

    def register_background_thread_object(bgt_object)
      finalizer = lambda do |bgt_object|
        bgt_object.stop!
      end
      LocalResourceRegistry.instance.register(bgt_object, finalizer)
    end

    def register_pool(pool)
      finalizer = lambda do |pool|
        if !pool.closed?
          pool.close(wait: true)
        end
      end
      LocalResourceRegistry.instance.register(pool, finalizer)
    end

    # Stop monitoring threads on the specified clients, after ensuring
    # each client has a writable server. Used for tests which assert on
    # global side effects like log messages being generated, to prevent
    # background threads from interfering with assertions.
    def stop_monitoring(*clients)
      clients.each do |client|
        client.cluster.next_primary
        client.cluster.close
        # We have tests that stop monitoring to reduce the noise happening in
        # background. These tests perform operations which requires the pools
        # to function. See also RUBY-3102.
        client.cluster.servers_list.each do |server|
          if pool = server.pool
            pool.instance_variable_set('@closed', false)
            # Stop the populator so that we don't have leftover threads.
            pool.instance_variable_get('@populator').stop!
          end
        end
      end
    end

    DNS_INTERFACES = [
      [:udp, "0.0.0.0", 5300],
      [:tcp, "0.0.0.0", 5300],
    ]

    def mock_dns(config)
      semaphore = Mongo::Semaphore.new

      thread = Thread.new do
        RubyDNS::run_server(DNS_INTERFACES) do
          config.each do |(query, type, *answers)|

            resource_cls = Resolv::DNS::Resource::IN.const_get(type.to_s.upcase)
            resources = answers.map do |answer|
              resource_cls.new(*answer)
            end
            match(query, resource_cls) do |req|
              req.add(resources)
            end
          end

          semaphore.signal
        end
      end

      semaphore.wait

      begin
        yield
      ensure
        10.times do
          if $last_async_task
            break
          end
          sleep 0.5
        end

        # Hack to stop the server - https://github.com/socketry/rubydns/issues/75
        if $last_async_task.nil?
          STDERR.puts "No async task - server never started?"
        else
          begin
            $last_async_task.stop
          rescue NoMethodError => e
            STDERR.puts "Error stopping async task: #{e}"
          end
        end

        thread.kill
        thread.join
      end
    end

    # Wait for snapshot reads to become available to prevent this error:
    # [246:SnapshotUnavailable]: Unable to read from a snapshot due to pending collection catalog changes; please retry the operation. Snapshot timestamp is Timestamp(1646666892, 4). Collection minimum is Timestamp(1646666892, 5) (on localhost:27017, modern retry, attempt 1)
    def wait_for_snapshot(db: nil, collection: nil, client: nil)
      client ||= authorized_client
      client = client.use(db) if db
      collection ||= 'any'
      start_time = Mongo::Utils.monotonic_time
      begin
        client.start_session(snapshot: true) do |session|
          client[collection].aggregate([{'$match': {any: true}}], session: session).to_a
        end
      rescue Mongo::Error::OperationFailure => e
        # Retry them as the server demands...
        if e.code == 246 # SnapshotUnavailable
          if Mongo::Utils.monotonic_time < start_time + 10
            retry
          end
        end
        raise
      end
    end

    # Make the server usable for operations after it was marked closed.
    # Used for tests that e.g. mock network operations to avoid interference
    # from server monitoring.
    def reset_pool(server)
      if pool = server.pool_internal
        pool.close
      end
      server.remove_instance_variable('@pool')
      server.pool.ready
    end
  end
end

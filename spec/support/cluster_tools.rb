require 'singleton'

# There is an undocumented {replSetStepUp: 1} command which can be used to
# ask a particular secondary to become a primary. It has existed since server
# 3.6 or earlier.
#
# Alternatively, to encourage a specific server to be selected, the recommended
# way is to set priority of that server higher. Changing priority requires
# reconfiguring the replica set, which in turn requires the replica set to
# have a primary.
#
# There are three timeouts that affect elections and stepdowns, when asking a
# server to step down:
#
# - secondaryCatchUpPeriodSecs - how long the existing primary will wait for
#   secondaries to catch up prior to actually stepping down.
# - replSetStepDown parameter - how long the existing primary will decline
#   getting elected as the new primary.
# - electionTimeoutMillis - how long, after a server notices that there is
#   no primary, that server will vote or call elections.
#
# These parameters must be configured in a certain way;
#
# - replSetStepDown should generally be higher than secondaryCatchUpPeriodSecs.
#   If a server is asked to step down and it spends all of its replSetStepDown
#   time waiting for secondaries to catch up, the stepdown itself will not
#   be performed and an error will be returned for the stepdown command.
# - secondaryCatchUpPeriodSecs + electionTimeoutMillis should be lower than
#   replSetStepDown, so that all of the other servers can participate in
#   the election prior to the primary which is stepping down becoming eligible
#   to vote and potentially getting reelected.
#
# Settings used by this test:
#
# - replSetStepDown = 4 seconds
# - secondaryCatchUpPeriodSecs = 2 seconds
# - electionTimeoutMillis = 1 second
#
# Recommended guidance for working elections:
# - Set priority of all nodes other than old primary and new desired primary
#   to 0
# - Turn off election handoff
# - Use stepdown & stepup commands (even when we don't care which server becomes
#   the new primary
# - Put step up command in retry loop

class ClusterTools
  include Singleton

  def force_step_down
    admin_client.database.command(
      replSetStepDown: 1, force: true)
  end

  # https://docs.mongodb.com/manual/reference/parameters/#param.enableElectionHandoff
  def set_election_handoff(value)
    unless [true, false].include?(value)
      raise ArgumentError, 'Value must be true or false'
    end

    direct_client_for_each_server do |client|
      client.use(:admin).database.command(setParameter: 1, enableElectionHandoff: value)
    end
  end

  # Sets election timeout to the specified value, in seconds.
  # Election timeout specifies how long nodes in a cluster wait to vote/ask
  # for elections once they lose connection with the active primary.
  #
  # This in theory generally safe to do in the test suite and leave the cluster
  # at the 1 second setting, because the tests are run against a local
  # deployment which shouldn't have any elections in it at all, unless we are
  # testing step down behavior in which case we want the election timeout
  # to be low. In practice a low election timeout results in intermittent
  # test failures, therefore the timeout should be restored to its default
  # value once step down tests are complete.
  def set_election_timeout(timeout)
    cfg = get_rs_config
    cfg['settings']['electionTimeoutMillis'] = timeout * 1000
    set_rs_config(cfg)
  end

  # Resets priorities on all replica set members to 1.
  #
  # Use at the end of a test run.
  def reset_priorities
    cfg = get_rs_config
    cfg['members'].each do |member|
      member['priority'] = 1
    end
    set_rs_config(cfg)
  end

  # Requests that the current primary in the RS steps down.
  def step_down
    admin_client.database.command(
      replSetStepDown: 4, secondaryCatchUpPeriodSecs: 2)
  rescue Mongo::Error::OperationFailure => e
    # While waiting for secondaries to catch up before stepping down, this node decided to step down for other reasons (189)
    if e.code == 189
      # success
    else
      raise
    end
  end

  # Attempts to elect the server at the specified address as the new primary
  # by asking it to step up.
  #
  # @param [ Mongo::Address ] address
  def step_up(address)
    client = direct_client(address)
    start = Time.now
    loop do
      begin
        client.database.command(replSetStepUp: 1)
        break
      rescue Mongo::Error::OperationFailure => e
        # Election failed. (125)
        if e.code == 125
          # Possible reason is the node we are trying to elect has blacklisted
          # itself. This is where {replSetFreeze: 0} should make it eligible
          # for election again but this seems to not always work.
        else
          raise
        end

        if Time.now > start + 10
          raise e
        end
      end
    end
    reset_server_states
  end

  # The recommended guidance for changing a primary is:
  #
  # - turn off election handoff
  # - pick a server to be the new primary
  # - set the target's priority to 10, existing primary's priority to 1,
  #   other servers' priorities to 0
  # - call step down on the existing primary
  # - call step up on the target in a loop until it becomes the primary
  def change_primary
    existing_primary = admin_client.cluster.next_primary
    existing_primary_address = existing_primary.address

    target = admin_client.cluster.servers_list.detect do |server|
      server.address != existing_primary_address
    end

    cfg = get_rs_config
    cfg['members'].each do |member|
      member['priority'] = case member['host']
      when existing_primary_address.to_s
        1
      when target.address.to_s
        10
      else
        0
      end
    end
    set_rs_config(cfg)

    if unfreeze_server(target.address)
      # Target server self-elected as primary, no further action is needed.
      return
    end

    step_down
    persistently_step_up(target.address)

    new_primary = admin_client.cluster.next_primary
    puts "#{Time.now} [CT] Primary changed to #{new_primary.address}"
  end

  def persistently_step_up(address)
    start = Time.now
    loop do
      puts "#{Time.now} [CT] Asking #{address} to step up"

      step_up(address)

      if admin_client.cluster.next_primary.address == address
        break
      end

      if Time.now - start > 10
        raise "Unable to get #{address} instated as primary after 10 seconds"
      end
    end
  end

  # Attempts to elect the server at the specified address as the new primary
  # by manipulating priorities.
  #
  # This method requires that there is an active primary in the replica set at
  # the time of the call (presumably a different one).
  #
  # @param [ Mongo::Address ] address
  def force_primary(address)
    current_primary = admin_client.cluster.next_primary
    if current_primary.address == address
      raise "Attempting to set primary to #{address} but it is already the primary"
    end
    encourage_primary(address)

    if unfreeze_server(address)
      # Target server self-elected as primary, no further action is needed.
      return
    end

    step_down
    persistently_step_up(address)
    admin_client.cluster.next_primary.unknown!
    new_primary = admin_client.cluster.next_primary
    if new_primary.address != address
      raise "Elected primary #{new_primary.address} is not what we wanted (#{address})"
    end
  end

  # Adjusts replica set configuration so that the next election is likely
  # to result in the server at the specified address becoming a primary.
  # Address should be a Mongo::Address object.
  #
  # This method requires that there is an active primary in the replica set at
  # the time of the call.
  #
  # @param [ Mongo::Address ] address
  def encourage_primary(address)
    existing_primary = admin_client.cluster.next_primary
    cfg = get_rs_config
    found = false
    cfg['members'].each do |member|
      if member['host'] == address.to_s
        member['priority'] = 10
        found = true
      elsif member['host'] == existing_primary.address.to_s
        member['priority'] = 1
      else
        member['priority'] = 0
      end
    end
    unless found
      raise "No RS member for #{address}"
    end

    set_rs_config(cfg)
  end

  # Allows the server at the specified address to run for elections and
  # potentially become a primary. Use after issuing a step down command
  # to clear the prohibtion on the stepped down server to be a primary.
  #
  # Returns true if the server at address became a primary, such that
  # a step up command is not necessary.
  def unfreeze_server(address)
    begin
      direct_client(address).use('admin').database.command(replSetFreeze: 0)
    rescue Mongo::Error::OperationFailure => e
      # Mongo::Error::OperationFailure: cannot freeze node when primary or running for election. state: Primary (95)
      if e.code == 95
        # The server we want to become primary may have already become the
        # primary by holding a spontaneous election and winning due to the
        # priorities we have set.
        admin_client.cluster.servers_list.each do |server|
          server.unknown!
        end
        if admin_client.cluster.next_primary.address == address
          puts "#{Time.now} [CT] Primary self-elected to #{address}"
          return true
        end
      end
      raise
    end
    false
  end

  def unfreeze_all
    admin_client.cluster.servers_list.each do |server|
      client = direct_client(server.address)
      # Primary refuses to be unfrozen with this message:
      # cannot freeze node when primary or running for election. state: Primary (95)
      if server != admin_client.cluster.next_primary
        client.use('admin').database.command(replSetFreeze: 0)
      end
    end
  end

  # Gets the current replica set configuration.
  def get_rs_config
    result = admin_client.database.command(replSetGetConfig: 1)
    doc = result.reply.documents.first
    if doc['ok'] != 1
      raise 'Failed to get RS config'
    end
    doc['config']
  end

  # Reconfigures the replica set with the specified configuration.
  # Automatically increases RS version in the process.
  def set_rs_config(config)
    config = config.dup
    config['version'] += 1
    result = admin_client.database.command(replSetReconfig: config)
    doc = result.reply.documents.first
    if doc['ok'] != 1
      raise 'Failed to reconfigure RS'
    end
  end

  def admin_client
    # Since we are triggering elections, we need to have a higher server
    # selection timeout applied. The default timeout for tests assumes a
    # stable deployment.
    @admin_client ||= ClientRegistry.instance.global_client('root_authorized_admin').
      with(server_selection_timeout: 15)
  end

  def direct_client(address, options = {})
    @direct_clients ||= {}
    cache_key = {address: address}.update(options)
    @direct_clients[cache_key] ||= ClientRegistry.instance.new_local_client(
      [address.to_s],
      SpecConfig.instance.test_options.merge(
        SpecConfig.instance.auth_options).merge(
        connect: :direct, server_selection_timeout: 10).merge(options))
  end

  private

  def each_server(&block)
    admin_client.cluster.servers_list.each(&block)
  end

  def direct_client_for_each_server(&block)
    each_server do |server|
      yield direct_client(server.address)
    end
  end

  def reset_server_states
    each_server do |server|
      server.unknown!
    end
  end
end

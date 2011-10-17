#!/usr/bin/ruby

require 'thread'

STDOUT.sync = true

unless defined? Mongo
  require File.join(File.dirname(__FILE__), '..', '..', 'lib', 'mongo')
end

class ReplSetManager

  attr_accessor :host, :start_port, :ports, :name, :mongods, :tags, :version

  def initialize(opts={})
    @start_port = opts[:start_port] || 30000
    @ports      = []
    @name       = opts[:name] || 'replica-set-foo'
    @host       = opts[:host]  || 'localhost'
    @retries    = opts[:retries] || 30
    @config     = {"_id" => @name, "members" => []}
    @durable    = opts.fetch(:durable, false)
    @path       = File.join(File.expand_path(File.dirname(__FILE__)), "data")
    @oplog_size = opts.fetch(:oplog_size, 32)
    @tags = [{"dc" => "ny", "rack" => "a", "db" => "main"},
             {"dc" => "ny", "rack" => "b", "db" => "main"},
             {"dc" => "sf", "rack" => "a", "db" => "main"}]

    @arbiter_count   = opts[:arbiter_count]   || 2
    @secondary_count = opts[:secondary_count] || 2
    @passive_count   = opts[:passive_count] || 0
    @primary_count   = 1

    @count = @primary_count + @passive_count + @arbiter_count + @secondary_count
    if @count > 7
      raise StandardError, "Cannot create a replica set with #{node_count} nodes. 7 is the max."
    end

    @mongods   = {}
    version_string = `mongod --version`
    version_string =~ /(\d\.\d\.\d)/
    @version = $1.split(".").map {|d| d.to_i }
  end

  def start_set
    begin
    con = Mongo::Connection.new(@host, @start_port)
      rescue Mongo::ConnectionFailure
      con = false
    end

    if con && ensure_up(1, con)
      should_start = false
      puts "** Replica set already started."
    else
      should_start = true
      system("killall mongod")
      puts "** Starting a replica set with #{@count} nodes"
    end

    n = 0
    (@primary_count + @secondary_count).times do
      init_node(n, should_start) do |attrs|
        if @version[0] >= 2
          attrs['tags'] = @tags[n % @tags.size]
        end
      end
      n += 1
    end

    @passive_count.times do
      init_node(n, should_start) do |attrs|
        attrs['priority'] = 0
      end
      n += 1
    end

    @arbiter_count.times do
      init_node(n, should_start) do |attrs|
        attrs['arbiterOnly'] = true
      end
      n += 1
    end

    if con && ensure_up(1, con)
      @mongods.each do |k, v|
        v['up'] = true
        v['pid'] = File.open(File.join(v['db_path'], 'mongod.lock')).read.strip
      end
    else
      initiate
      ensure_up
    end
  end

  def cleanup_set
    system("killall mongod")
    @count.times do |n|
      system("rm -rf #{@mongods[n]['db_path']}")
    end
  end

  def init_node(n, should_start=true)
    @mongods[n] ||= {}
    port = @start_port + n
    @ports << port
    @mongods[n]['port'] = port
    @mongods[n]['db_path'] = get_path("rs-#{port}")
    @mongods[n]['log_path'] = get_path("log-#{port}")
    @mongods[n]['start'] = start_cmd(n)

    if should_start
      system("rm -rf #{@mongods[n]['db_path']}")
      system("mkdir -p #{@mongods[n]['db_path']}")
      start(n)
    end

    member = {'_id' => n, 'host' => "#{@host}:#{@mongods[n]['port']}"}

    if block_given?
      custom_attrs = {}
      yield custom_attrs
      member.merge!(custom_attrs)
      @mongods[n].merge!(custom_attrs)
    end

    @config['members'] << member
  end

  def journal_switch
    if @version[0] >= 2
      if @durable
        "--journal"
      else
        "--nojournal"
      end
    elsif @durable
      "--journal"
    end
  end

  def start_cmd(n)
    @mongods[n]['start'] = "mongod --replSet #{@name} --logpath '#{@mongods[n]['log_path']}' " +
     "--oplogSize #{@oplog_size} #{journal_switch} --dbpath #{@mongods[n]['db_path']} --port #{@mongods[n]['port']} --fork"
    @mongods[n]['start'] += " --dur" if @durable
    @mongods[n]['start']
  end

  def remove_secondary_node
    primary = get_node_with_state(1)
    con = get_connection(primary)
    config = con['local']['system.replset'].find_one
    secondary = get_node_with_state(2)
    host_port = "#{@host}:#{@mongods[secondary]['port']}"
    kill(secondary)
    @mongods.delete(secondary)
    @config['members'].reject! {|m| m['host'] == host_port}
    @config['version'] = config['version'] + 1

    begin
      con['admin'].command({'replSetReconfig' => @config})
    rescue Mongo::ConnectionFailure
    end

    con.close

    return secondary
  end

  def add_node(n=nil)
    primary = get_node_with_state(1)
    con = get_connection(primary)
    init_node(n || @mongods.length)

    config = con['local']['system.replset'].find_one
    @config['version'] = config['version'] + 1

    # We expect a connection failure on reconfigure here.
    begin
      con['admin'].command({'replSetReconfig' => @config})
    rescue Mongo::ConnectionFailure
    end

    con.close
    ensure_up
  end

  def kill(node, signal=2)
    pid = @mongods[node]['pid']
    puts "** Killing node with pid #{pid} at port #{@mongods[node]['port']}"
    system("kill #{pid}")
    @mongods[node]['up'] = false
  end

  def kill_primary(signal=2)
    node = get_node_with_state(1)
    kill(node, signal)
    return node
  end

  # Note that we have to rescue a connection failure
  # when we run the StepDown command because that
  # command will close the connection.
  def step_down_primary
    primary = get_node_with_state(1)
    con = get_connection(primary)
    begin
      con['admin'].command({'replSetStepDown' => 90})
    rescue Mongo::ConnectionFailure
    end
    con.close
  end

  def kill_secondary
    node = get_node_with_state(2)
    kill(node)
    return node
  end

  def kill_all_secondaries
    nodes = get_all_nodes_with_state(2)
    if nodes
      nodes.each do |n|
        kill(n)
      end
    end
  end

  def restart_killed_nodes
    nodes = @mongods.keys.select do |key|
      @mongods[key]['up'] == false
    end

    nodes.each do |node|
      start(node)
    end

    ensure_up
  end

  def get_node_from_port(port)
    @mongods.keys.detect { |key| @mongods[key]['port'] == port }
  end

  def start(node)
    system(@mongods[node]['start'])
    @mongods[node]['up'] = true
    sleep(0.5)
    @mongods[node]['pid'] = File.open(File.join(@mongods[node]['db_path'], 'mongod.lock')).read.strip
  end
  alias :restart :start

  def ensure_up(n=nil, connection=nil)
    print "** Ensuring members are up..."

    attempt(n) do
      con = connection || get_connection
      status = con['admin'].command({'replSetGetStatus' => 1})
      print "."
      if status['members'].all? { |m| m['health'] == 1 &&
         [1, 2, 7].include?(m['state']) } &&
         status['members'].any? { |m| m['state'] == 1 }
        print "all members up!\n\n"
        con.close
        return status
      else
        con.close
        raise Mongo::OperationFailure
      end
    end

    return false
  end

  def primary
    nodes = get_all_host_pairs_with_state(1)
    nodes.empty? ? nil : nodes[0]
  end

  def secondaries
    get_all_host_pairs_with_state(2)
  end

  def arbiters
    get_all_host_pairs_with_state(7)
  end

  # String used for adding a shard via mongos
  # using the addshard command.
  def shard_string
    str = "#{@name}/"
    str << @mongods.map do |k, mongod|
      "#{@host}:#{mongod['port']}"
    end.join(',')
    str
  end

  private

  def initiate
    puts "Initiating replica set..."
    con = get_connection

    attempt do
      p con['admin'].command({'replSetInitiate' => @config})
    end

    con.close
  end

  def get_all_nodes_with_state(state)
    status = ensure_up
    nodes = status['members'].select {|m| m['state'] == state}
    nodes = nodes.map do |node|
      host_port = node['name'].split(':')
      port = host_port[1] ? host_port[1].to_i : 27017
      @mongods.keys.detect {|key| @mongods[key]['port'] == port}
    end

    nodes == [] ? false : nodes
  end

  def get_node_with_state(state)
    status = ensure_up
    node = status['members'].detect {|m| m['state'] == state}
    if node
      host_port = node['name'].split(':')
      port = host_port[1] ? host_port[1].to_i : 27017
      key = @mongods.keys.detect {|n| @mongods[n]['port'] == port}
      return key
    else
      return false
    end
  end

  def get_all_host_pairs_with_state(state)
    status = ensure_up
    nodes = status['members'].select {|m| m['state'] == state}
    nodes.map do |node|
      host_port = node['name'].split(':')
      port = host_port[1] ? host_port[1].to_i : 27017
      [host, port]
    end
  end

  def get_connection(node=nil)
    con = attempt do
      if !node
        node = @mongods.keys.detect {|key| !@mongods[key]['arbiterOnly'] && @mongods[key]['up'] }
      end
      con = Mongo::Connection.new(@host, @mongods[node]['port'], :slave_ok => true)
    end

    return con
  end

  def get_path(name)
    File.join(@path, name)
  end

  def attempt(retries=nil)
    raise "No block given!" unless block_given?
    count = 0

    while count < (retries || @retries) do
      begin
        return yield
        rescue Mongo::OperationFailure, Mongo::ConnectionFailure => ex
          sleep(2)
          count += 1
      end
    end

    puts "NO MORE ATTEMPTS"
    raise ex
  end

end

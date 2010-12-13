#!/usr/bin/ruby

STDOUT.sync = true

unless defined? Mongo
  require File.join(File.dirname(__FILE__), '..', '..', 'lib', 'mongo')
end

class ReplSetManager

  attr_accessor :host, :start_port, :ports

  def initialize(opts={})
    @start_port = opts[:start_port] || 30000
    @ports      = []
    @name       = opts[:name] || 'replica-set-foo'
    @count      = opts[:count] || 3
    @host       = opts[:host]  || 'localhost'
    @retries    = opts[:retries] || 60
    @config     = {"_id" => @name, "members" => []}
    @path       = File.join(File.expand_path(File.dirname(__FILE__)), "data")

    @mongods   = {}
  end

  def start_set
    puts "Starting a replica set with #{@count} nodes"

    system("killall mongod")

    @count.times do |n|
      @mongods[n] ||= {}
      port = @start_port + n
      @ports << port
      @mongods[n]['port'] = port
      @mongods[n]['db_path'] = get_path("rs-#{port}")
      @mongods[n]['log_path'] = get_path("log-#{port}")
      system("rm -rf #{@mongods[n]['db_path']}")
      system("mkdir -p #{@mongods[n]['db_path']}")

      @mongods[n]['start'] = "mongod --replSet #{@name} --logpath '#{@mongods[n]['log_path']}' " +
       " --dbpath #{@mongods[n]['db_path']} --port #{@mongods[n]['port']} --fork"

      start(n)

      member = {'_id' => n, 'host' => "#{@host}:#{@mongods[n]['port']}"}
      if n == @count-1
        @mongods[n]['arbiter'] = true
        member['arbiterOnly'] = true
      end

      @config['members'] << member
    end

    init
    ensure_up
  end

  def kill(node)
    system("kill -2 #{@mongods[node]['pid']}")
    @mongods[node]['up'] = false
    sleep(1)
  end

  def kill_primary
    node = get_node_with_state(1)
    kill(node)
    return node
  end

  def kill_secondary
    node = get_node_with_state(2)
    kill(node)
    return node
  end

  def start(node)
    system(@mongods[node]['start'])
    @mongods[node]['up'] = true
    sleep(1)
    @mongods[node]['pid'] = File.open(File.join(@mongods[node]['db_path'], 'mongod.lock')).read.strip
  end
  alias :restart :start

  def ensure_up
    print "Ensuring members are up..."
    @con = get_connection

    attempt(Mongo::OperationFailure) do
      status = @con['admin'].command({'replSetGetStatus' => 1})
      print "."
      if status['members'].all? { |m| [1, 2, 7].include?(m['state']) }
        puts "All members up!"
        return status
      else
        raise Mongo::OperationFailure
      end
    end
  end

  private

  def init
    get_connection

    attempt(Mongo::OperationFailure) do
      @con['admin'].command({'replSetInitiate' => @config})
    end
  end

  def get_node_with_state(state)
    status = ensure_up
    node = status['members'].detect {|m| m['state'] == state}
    if node
      host_port = node['name'].split(':')
      port = host_port[1] ? host_port[1].to_i : 27017
      key = @mongods.keys.detect {|key| @mongods[key]['port'] == port}
      return key
    else
      return false
    end
  end

  def get_connection
    attempt(Mongo::ConnectionFailure) do
      node = @mongods.keys.detect {|key| !@mongods[key]['arbiter'] && @mongods[key]['up'] }
      @con = Mongo::Connection.new(@host, @mongods[node]['port'], :slave_ok => true)
    end

    return @con
  end

  def get_path(name)
    File.join(@path, name)
  end

  def attempt(exception)
    raise "No block given!" unless block_given?
    count = 0

    while count < @retries do
      begin
        yield
        return
        rescue exception
          sleep(1)
          count += 1
      end
    end

    raise exception
  end

end

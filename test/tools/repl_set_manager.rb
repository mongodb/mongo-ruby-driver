#!/usr/bin/ruby

require 'rubygems'
require 'mongo'

class ReplSetManager

  def initialize(opts={})
    @start_port = opts[:start_port] || 30000
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
      @mongods[n]['db_path'] = get_path("rs-#{n}")
      @mongods[n]['log_path'] = get_path("log-#{n}")
      system("rm -rf #{@mongods[n]['db_path']}")
      system("mkdir -p #{@mongods[n]['db_path']}")

      @mongods[n]['port'] = @start_port + n
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

    p @mongods
    init
    ensure_up
  end

  def kill(node)
    system("kill -2 #{@mongods[node]['pid']}")
    @mongods[node]['up'] = false
    sleep(1)
  end

  def start(node)
    system(@mongods[node]['start'])
    @mongods[node]['up'] = true
    sleep(1)
    @mongods[node]['pid'] = File.open(File.join(@mongods[node]['db_path'], 'mongod.lock')).read.strip
  end
  alias :restart :start

  def ensure_up
    @con = get_connection
    p @con

    attempt(Mongo::OperationFailure) do
      status = @con['admin'].command({'replSetGetStatus' => 1})
      p status
      if status['members'].all? { |m| [1, 2, 7].include?(m['state']) }
        puts "All members up!"
        return
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

  def get_connection
    attempt(Mongo::ConnectionFailure) do
      node = @mongods.keys.detect {|key| !@mongods[key]['arbiter'] && @mongods[key]['up'] }
      p @mongods[node]['port']
      p node
      @con = Mongo::Connection.new(@host, @mongods[node]['port'], :slave_ok => true)
    end

    return @con
  end

  def get_path(name)
    p @path
    j = File.join(@path, name)
    p j
    j
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

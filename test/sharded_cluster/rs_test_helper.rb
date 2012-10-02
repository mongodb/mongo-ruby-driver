$:.unshift(File.expand_path('../../lib', File.dirname(__FILE__)))
require File.expand_path("../../test_helper", __FILE__)
require 'test/tools/mongo_config'

class Test::Unit::TestCase
  # Ensure sharded cluster is available as an instance variable and that
  # a new set is spun up for each TestCase class
  def ensure_rs
    if defined?(@@current_class) and @@current_class == self.class
      @@rs.start
    else
      @@current_class = self.class
      dbpath = 'rs'
      opts = Mongo::Config::DEFAULT_REPLICA_SET.merge(:dbpath => dbpath)
      #debug 1, opts
      config = Mongo::Config.cluster(opts)
      #debug 1, config
      @@rs = Mongo::Config::ClusterManager.new(config)
      @@rs.start
    end
    @rs = @@rs
  end

  # Generic code for rescuing connection failures and retrying operations.
  # This could be combined with some timeout functionality.
  def rescue_connection_failure(max_retries=30)
    retries = 0
    begin
      yield
    rescue Mongo::ConnectionFailure => ex
      #puts "Rescue attempt #{retries}: from #{ex}"
      retries += 1
      raise ex if retries > max_retries
      sleep(2)
      retry
    end
  end

end

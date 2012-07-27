$:.unshift(File.expand_path('../../lib', File.dirname(__FILE__)))
require File.expand_path("../../test_helper", __FILE__)
require 'test/tools/mongo_config'

class Test::Unit::TestCase
  # Ensure sharded cluster is available as an instance variable and that
  # a new set is spun up for each TestCase class
  def ensure_sc
    if defined?(@@current_class) and @@current_class == self.class
      @@sc.start
    else
      @@current_class = self.class
      dbpath = 'sc'
      opts = Mongo::Config::DEFAULT_SHARDED_SIMPLE.merge(:dbpath => dbpath).merge(:routers => 4)
      #debug 1, opts
      config = Mongo::Config.cluster(opts)
      #debug 1, config
      @@sc = Mongo::Config::ClusterManager.new(config)
      @@sc.start
    end
    @sc = @@sc
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

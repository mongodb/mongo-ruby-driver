$:.unshift(File.join(File.dirname(__FILE__), '..', 'lib'))
require './test/test_helper'
require './test/tools/repl_set_manager'

module ReplicaSetTest

  def self.rs
    unless defined?(@rs)
      @rs = ReplSetManager.new
      @rs.start_set
    end
    @rs
  end

  def rs
    ReplicaSetTest.rs
  end

  # Generic code for rescuing connection failures and retrying operations.
  # This could be combined with some timeout functionality.
  def rescue_connection_failure(max_retries=30)
    retries = 0
    begin
      yield
    rescue Mongo::ConnectionFailure => ex
      puts "Rescue attempt #{retries}: from #{ex}"
      retries += 1
      raise ex if retries > max_retries
      sleep(2)
      retry
    end
  end
end

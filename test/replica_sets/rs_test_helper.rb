$:.unshift(File.join(File.dirname(__FILE__), '..', 'lib'))
require './test/test_helper'
require './test/tools/repl_set_manager'

unless defined? RS
  RS = ReplSetManager.new
  RS.start_set
end

class Test::Unit::TestCase

  # Generic code for rescuing connection failures and retrying operations.
  # This could be combined with some timeout functionality.
  def rescue_connection_failure(max_retries=60)
    success = false
    tries   = 0
    while !success && tries < max_retries
      begin
        yield
        success = true
      rescue Mongo::ConnectionFailure
        puts "Rescue attempt #{tries}\n"
        tries += 1
        sleep(1)
      end
    end
  end

end

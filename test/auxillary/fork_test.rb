require 'test_helper'
require 'mongo'

class ForkTest < Test::Unit::TestCase
  include Mongo

  def setup
    @client = standard_connection
  end

  def test_fork
    # Now insert some data
    10.times do |n|
      @client[MONGO_TEST_DB]['nums'].insert({:a => n})
    end

    # Now fork. You'll almost always see an exception here.
    if !Kernel.fork
      10.times do
        assert @client[MONGO_TEST_DB]['nums'].find_one
      end
    else
      10.times do
        assert @client[MONGO_TEST_DB]['nums'].find_one
      end
    end
  end
end

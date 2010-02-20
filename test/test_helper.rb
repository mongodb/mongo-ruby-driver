$:.unshift(File.join(File.dirname(__FILE__), '..', 'lib'))
require 'rubygems' if ENV['C_EXT']
require 'mongo'
require 'test/unit'

begin
  require 'rubygems'
  require 'mocha'
  rescue LoadError
    puts <<MSG

This test suite requires mocha.
You can install it as follows:
  gem install mocha

MSG
    exit
end

require 'mongo_ext/cbson' if ENV['C_EXT']

# NOTE: most tests assume that MongoDB is running.
class Test::Unit::TestCase
  include Mongo

  # Generic code for rescuing connection failures and retrying operations.
  # This could be combined with some timeout functionality.
  def rescue_connection_failure
    success = false
    while !success
      begin
        yield
        success = true
      rescue Mongo::ConnectionFailure
        puts "Rescuing"
        sleep(1)
      end
    end
  end
end

# shoulda-mini
# based on test/spec/mini 5
# http://gist.github.com/307649
# chris@ozmm.org
#
def context(*args, &block)
  return super unless (name = args.first) && block
  require 'test/unit'
  klass = Class.new(Test::Unit::TestCase) do
    def self.should(name, &block)
      define_method("test_#{name.to_s.gsub(/\W/,'_')}", &block) if block
    end
    def self.xshould(*args) end
    def self.context(*args, &block) instance_eval(&block) end
    def self.setup(&block)
      define_method(:setup) { self.class.setups.each { |s| instance_eval(&s) } }
      setups << block
    end
    def self.setups; @setups ||= [] end
    def self.teardown(&block) define_method(:teardown, &block) end
  end
  (class << klass; self end).send(:define_method, :name) { name.gsub(/\W/,'_') }
  klass.class_eval do
    include Mongo
  end
  klass.class_eval &block
end

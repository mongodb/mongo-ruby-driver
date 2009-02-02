$LOAD_PATH[0,0] = File.join(File.dirname(__FILE__), '../..', 'lib')
require 'mongo'

DEFAULT_HOST = '127.0.0.1'
DEFAULT_PORT = 27017
DEFAULT_DB = 'driver_test_framework'

include XGen::Mongo::Driver

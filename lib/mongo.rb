$LOAD_PATH << File.join(File.dirname(__FILE__), '..', 'lib')

require 'mongo/types/binary'
require 'mongo/types/code'
require 'mongo/types/dbref'
require 'mongo/types/objectid'
require 'mongo/types/regexp_of_holding'

require 'mongo/util/support'
require 'mongo/util/conversions'
require 'mongo/util/server_version'

require 'mongo/errors'
require 'mongo/constants'
require 'mongo/connection'
require 'mongo/db'
require 'mongo/cursor'
require 'mongo/collection'
require 'mongo/admin'

module Mongo
  ASCENDING = 1
  DESCENDING = -1

  VERSION = "0.16"
end

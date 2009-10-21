require 'mongo/types/binary'
require 'mongo/types/dbref'
require 'mongo/types/objectid'
require 'mongo/types/regexp_of_holding'

require 'mongo/util/conversions'

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

  VERSION = "1.15.1"
end

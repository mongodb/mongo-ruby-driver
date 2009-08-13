require 'mongo/types/binary'
require 'mongo/types/dbref'
require 'mongo/types/objectid'
require 'mongo/types/regexp_of_holding'
require 'mongo/types/undefined'

require 'mongo/errors'
require 'mongo/mongo'
require 'mongo/message'
require 'mongo/db'
require 'mongo/cursor'
require 'mongo/collection'
require 'mongo/admin'

module XGen
  module Mongo
    ASCENDING = 1
    DESCENDING = -1
  end
end

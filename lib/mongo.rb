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

module Mongo
  ASCENDING = 1
  DESCENDING = -1
end

# DEPRECATED - the XGen namespace is deprecated and will be removed - use Mongo or GridFS instead
MongoCopy = Mongo
module XGen
  require 'mongo/gridfs'
  GridFSCopy = GridFS

  def self.included(other_module)
    warn "the XGen module is deprecated and will be removed - use Mongo or GridFS instead (included from: #{other_module})"
  end

  module Mongo
    include MongoCopy

    def self.included(other_module)
      warn "the XGen::Mongo module is deprecated and will be removed - use Mongo instead (included from: #{other_module})"
    end

    module Driver
      include MongoCopy

      def self.included(other_module)
        warn "the XGen::Mongo::Driver module is deprecated and will be removed - use Mongo instead (included from: #{other_module})"
      end
    end
    module GridFS
      include GridFSCopy

      def self.included(other_module)
        warn "the XGen::Mongo::GridFS module is deprecated and will be removed - use GridFS instead (included from: #{other_module})"
      end
    end
  end
end

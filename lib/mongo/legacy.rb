module Mongo
  module LegacyWriteConcern
    @legacy_write_concern = true

    def safe=(value)
      @write_concern = value
    end

    def safe
      if @write_concern[:w] == 0
        return false
      elsif @write_concern[:w] == 1
        return true
      else
        return @write_concern
      end
    end

    def self.from_uri(uri = ENV['MONGODB_URI'], extra_opts={})
      parser = URIParser.new uri
      parser.connection(extra_opts, true)
    end
  end
end

module Mongo
  # @deprecated Use Mongo::MongoClient instead. Support will be removed after v2.0
  # Please see old documentation for the Connection class
  class Connection < MongoClient
    include Mongo::LegacyWriteConcern

    def initialize(*args)
      if args.last.is_a?(Hash)
        opts = args.pop
        write_concern_from_legacy(opts)
        args.push(opts)
      end
      super
    end
  end

  # @deprecated Use Mongo::MongoReplicaSetClient instead. Support will be removed after v2.0
  # Please see old documentation for the ReplSetConnection class
  class ReplSetConnection < MongoReplicaSetClient
    include Mongo::LegacyWriteConcern

    def initialize(*args)
      if args.last.is_a?(Hash)
        opts = args.pop
        write_concern_from_legacy(opts)
        args.push(opts)
      end
      super
    end
  end

  # @deprecated Use Mongo::MongoShardedClient instead. Support will be removed after v2.0
  # Please see old documentation for the ShardedConnection class
  class ShardedConnection < MongoShardedClient
    include Mongo::LegacyWriteConcern

    def initialize(*args)
      if args.last.is_a?(Hash)
        opts = args.pop
        write_concern_from_legacy(opts)
        args.push(opts)
      end
      super
    end
  end
end
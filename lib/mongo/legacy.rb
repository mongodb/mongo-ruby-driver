# Copyright (C) 2009-2013 MongoDB, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

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
  # @deprecated Use Mongo::MongoClient instead. Support will be removed after
  #   v2.0. Please see old documentation for the Connection class.
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

  # @deprecated Use Mongo::MongoReplicaSetClient instead. Support will be
  #   removed after v2.0. Please see old documentation for the
  #   ReplSetConnection class.
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

  # @deprecated Use Mongo::MongoShardedClient instead. Support will be removed
  #   after v2.0. Please see old documentation for the ShardedConnection class.
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

  class MongoClient
    # @deprecated This method is no longer in use and never needs to be called
    #   directly. Support will be removed after v2.0
    # Authentication of sockets is handled upon checkout and checkin.
    def authenticate_pools
    end

    # @deprecated This method is no longer in use and never needs to be called
    #   directly. Support will be removed after v2.0
    # Authentication of sockets is handled upon checkout and checkin.
    def logout_pools(database)
    end

    # @deprecated This method is no longer in use and never needs to be called
    #   directly. Support will be removed after v2.0
    def apply_saved_authentication
      true
    end
  end

  class MongoReplicaSetClient
    # @deprecated This method is no longer in use and never needs to be called
    #   directly. Support will be removed after v2.0
    # Authentication of sockets is handled upon checkout and checkin.
    def authenticate_pools
    end

    # @deprecated This method is no longer in use and never needs to be called
    #   directly. Support will be removed after v2.0
    # Authentication of sockets is handled upon checkout and checkin.
    def logout_pools(database)
    end
  end

  class DB
    # @deprecated Please use MongoClient#issue_authentication instead. Support
    #   will be removed after v2.0
    def issue_authentication(username, password, save_auth=true, opts={})
      auth = Authentication.validate_credentials({
        :db_name  => self.name,
        :username => username,
        :password => password
      })
      opts[:save_auth] = save_auth
      @client.issue_authentication(auth, opts)
    end

    # @deprecated Please use MongoClient#issue_logout instead. Support will be
    #   removed after v2.0
    def issue_logout(opts={})
      @client.issue_logout(self.name, opts)
    end
  end
end

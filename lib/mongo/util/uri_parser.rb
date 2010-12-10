# encoding: UTF-8

# --
# Copyright (C) 2008-2010 10gen Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
# ++

module Mongo
  module URIParser

    DEFAULT_PORT = 27017
    MONGODB_URI_MATCHER = /(([-_.\w\d]+):([-_\w\d]+)@)?([-.\w\d]+)(:([\w\d]+))?(\/([-\d\w]+))?/
    MONGODB_URI_SPEC = "mongodb://[username:password@]host1[:port1][,host2[:port2],...[,hostN[:portN]]][/database]"

    extend self

    # Parse a MongoDB URI. This method is used by Connection.from_uri.
    # Returns an array of nodes and an array of db authorizations, if applicable.
    #
    # @private
    def parse(string)
      if string =~ /^mongodb:\/\//
        string = string[10..-1]
      else
        raise MongoArgumentError, "MongoDB URI must match this spec: #{MONGODB_URI_SPEC}"
      end

      nodes = []
      auths = []
      specs = string.split(',')
      specs.each do |spec|
        matches  = MONGODB_URI_MATCHER.match(spec)
        if !matches
          raise MongoArgumentError, "MongoDB URI must match this spec: #{MONGODB_URI_SPEC}"
        end

        uname = matches[2]
        pwd   = matches[3]
        host  = matches[4]
        port  = matches[6] || DEFAULT_PORT
        if !(port.to_s =~ /^\d+$/)
          raise MongoArgumentError, "Invalid port #{port}; port must be specified as digits."
        end
        port  = port.to_i
        db    = matches[8]

        if uname && pwd && db
          auths << {'db_name' => db, 'username' => uname, 'password' => pwd}
        elsif uname || pwd || db
          raise MongoArgumentError, "MongoDB URI must include all three of username, password, " +
            "and db if any one of these is specified."
        end

        nodes << [host, port]
      end

      [nodes, auths]
    end
  end
end

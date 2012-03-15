# encoding: UTF-8

# --
# Copyright (C) 2008-2011 10gen Inc.
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
  class URIParser

    DEFAULT_PORT = 27017

    USER_REGEX = /([-.\w:]+)/
    PASS_REGEX = /([^@,]+)/
    AUTH_REGEX = /(#{USER_REGEX}:#{PASS_REGEX}@)?/

    HOST_REGEX = /([-.\w]+)/
    PORT_REGEX = /(?::(\w+))?/
    NODE_REGEX = /((#{HOST_REGEX}#{PORT_REGEX},?)+)/

    PATH_REGEX = /(?:\/([-\w]+))?/

    MONGODB_URI_MATCHER = /#{AUTH_REGEX}#{NODE_REGEX}#{PATH_REGEX}/
    MONGODB_URI_SPEC = "mongodb://[username:password@]host1[:port1][,host2[:port2],...[,hostN[:portN]]][/[database][?options]]"

    SPEC_ATTRS = [:nodes, :auths]
    OPT_ATTRS  = [:connect, :replicaset, :slaveok, :safe, :w, :wtimeout, :fsync, :journal, :connecttimeoutms, :sockettimeoutms, :wtimeoutms]

    OPT_VALID  = {:connect          => lambda {|arg| ['direct', 'replicaset'].include?(arg)},
                  :replicaset       => lambda {|arg| arg.length > 0},
                  :slaveok          => lambda {|arg| ['true', 'false'].include?(arg)},
                  :safe             => lambda {|arg| ['true', 'false'].include?(arg)},
                  :w                => lambda {|arg| arg =~ /^\d+$/ },
                  :wtimeout         => lambda {|arg| arg =~ /^\d+$/ },
                  :fsync            => lambda {|arg| ['true', 'false'].include?(arg)},
                  :journal          => lambda {|arg| ['true', 'false'].include?(arg)},
                  :connecttimeoutms => lambda {|arg| arg =~ /^\d+$/ },
                  :sockettimeoutms  => lambda {|arg| arg =~ /^\d+$/ },
                  :wtimeoutms       => lambda {|arg| arg =~ /^\d+$/ }
                 }

    OPT_ERR    = {:connect          => "must be 'direct' or 'replicaset'",
                  :replicaset       => "must be a string containing the name of the replica set to connect to",
                  :slaveok          => "must be 'true' or 'false'",
                  :safe             => "must be 'true' or 'false'",
                  :w                => "must be an integer specifying number of nodes to replica to",
                  :wtimeout         => "must be an integer specifying milliseconds",
                  :fsync            => "must be 'true' or 'false'",
                  :journal          => "must be 'true' or 'false'",
                  :connecttimeoutms => "must be an integer specifying milliseconds",
                  :sockettimeoutms  => "must be an integer specifying milliseconds",
                  :wtimeoutms       => "must be an integer specifying milliseconds"
                 }

    OPT_CONV   = {:connect          => lambda {|arg| arg},
                  :replicaset       => lambda {|arg| arg},
                  :slaveok          => lambda {|arg| arg == 'true' ? true : false},
                  :safe             => lambda {|arg| arg == 'true' ? true : false},
                  :w                => lambda {|arg| arg.to_i},
                  :wtimeout         => lambda {|arg| arg.to_i},
                  :fsync            => lambda {|arg| arg == 'true' ? true : false},
                  :journal          => lambda {|arg| arg == 'true' ? true : false},
                  :connecttimeoutms => lambda {|arg| arg.to_f / 1000 }, # stored as seconds
                  :sockettimeoutms  => lambda {|arg| arg.to_f / 1000 }, # stored as seconds
                  :wtimeoutms       => lambda {|arg| arg.to_i }
                 }

    attr_reader :nodes, :auths, :connect, :replicaset, :slaveok, :safe, :w, :wtimeout, :fsync, :journal, :connecttimeoutms, :sockettimeoutms, :wtimeoutms, :database_name

    # Parse a MongoDB URI. This method is used by Connection.from_uri.
    # Returns an array of nodes and an array of db authorizations, if applicable.
    #
    # Note: passwords can contain any character except for a ','.
    #
    # @core connections
    def initialize(string)
      if string =~ /^mongodb:\/\//
        string = string[10..-1]
      else
        raise MongoArgumentError, "MongoDB URI must match this spec: #{MONGODB_URI_SPEC}"
      end

      hosts, opts = string.split('?')
      parse_hosts(hosts)
      parse_options(opts)
      configure_connect
    end

    def connection_options
      opts = {}

      if (@w || @journal || @wtimeout || @fsync || @wtimeoutms) && !@safe
        raise MongoArgumentError, "Safe must be true if w, journal, wtimeoutMS, or fsync is specified"
      end

      if @safe
        if @w || @journal || @wtimeout || @fsync || @wtimeoutms
          safe_opts = {}
          safe_opts[:w] = @w if @w
          safe_opts[:j] = @journal if @journal
          
          if @wtimeout
            warn "Using wtimeout in a URI is deprecated, please use wtimeoutMS. It will be removed in v2.0."
            safe_opts[:wtimeout] = @wtimeout
          end
          
          if @wtimeoutms
            safe_opts[:wtimeout] = @wtimeoutms
          end
          
          safe_opts[:fsync] = @fsync if @fsync
        else
          safe_opts = true
        end

        opts[:safe] = safe_opts
      end
      
      if @connecttimeoutms
        opts[:connect_timeout] = @connecttimeoutms
      end
      
      if @sockettimeoutms
        opts[:op_timeout] = @sockettimeoutms
      end

      if @slaveok
        if @connect == 'direct'
          opts[:slave_ok] = true
        else
          opts[:read_secondary] = true
        end
      end

      opts[:rs_name] = @replicaset if @replicaset

      opts
    end

    private

    def parse_hosts(uri_without_proto)
      @nodes = []
      @auths = []

      matches = MONGODB_URI_MATCHER.match(uri_without_proto)

      if !matches
        raise MongoArgumentError, "MongoDB URI must match this spec: #{MONGODB_URI_SPEC}"
      end

      uname    = matches[2]
      pwd      = matches[3]
      hosturis = matches[4].split(',')
      db       = matches[8]

      @database_name = db

      hosturis.each do |hosturi|
        # If port is present, use it, otherwise use default port
        host, port = hosturi.split(':') + [DEFAULT_PORT]

        if !(port.to_s =~ /^\d+$/)
          raise MongoArgumentError, "Invalid port #{port}; port must be specified as digits."
        end

        port = port.to_i

        @nodes << [host, port]
      end

      if uname && pwd && db
        auths << {'db_name' => db, 'username' => uname, 'password' => pwd}
      elsif uname || pwd
        raise MongoArgumentError, "MongoDB URI must include username, password, "
          "and db if username and password are specified."
      end

      # The auths are repeated for each host in a replica set
      @auths *= hosturis.length
    end

    # This method uses the lambdas defined in OPT_VALID and OPT_CONV to validate
    # and convert the given options.
    def parse_options(opts)
      # initialize instance variables for available options
      OPT_VALID.keys.each { |k| instance_variable_set("@#{k}", nil) }

      return unless opts

      separator = opts.include?('&') ? '&' : ';'
      opts.split(separator).each do |attr|
        key, value = attr.split('=')
        key = key.downcase.to_sym
        value = value.strip.downcase
        if !OPT_ATTRS.include?(key)
          raise MongoArgumentError, "Invalid Mongo URI option #{key}"
        end

        if OPT_VALID[key].call(value)
          instance_variable_set("@#{key}", OPT_CONV[key].call(value))
        else
          raise MongoArgumentError, "Invalid value for #{key}: #{OPT_ERR[key]}"
        end
      end
    end

    def configure_connect
      if !@connect
        if @nodes.length > 1
          @connect = 'replicaset'
        else
          @connect = 'direct'
        end
      end

      if @connect == 'direct' && @replicaset
        raise MongoArgumentError, "If specifying a replica set name, please also specify that connect=replicaset"
      end
    end
  end
end

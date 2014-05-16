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

require 'cgi'
require 'uri'

module Mongo
  class URIParser

    AUTH_REGEX = /((.+)@)?/

    HOST_REGEX = /([-.\w]+)|(\[[^\]]+\])/
    PORT_REGEX = /(?::(\w+))?/
    NODE_REGEX = /((#{HOST_REGEX}#{PORT_REGEX},?)+)/

    PATH_REGEX = /(?:\/([-\w]+))?/

    MONGODB_URI_MATCHER = /#{AUTH_REGEX}#{NODE_REGEX}#{PATH_REGEX}/
    MONGODB_URI_SPEC = "mongodb://[username:password@]host1[:port1][,host2[:port2],...[,hostN[:portN]]][/[database][?options]]"

    SPEC_ATTRS = [:nodes, :auths]

    READ_PREFERENCES = {
      'primary'            => :primary,
      'primarypreferred'   => :primary_preferred,
      'secondary'          => :secondary,
      'secondarypreferred' => :secondary_preferred,
      'nearest'            => :nearest
    }

    OPT_ATTRS  = [
      :authmechanism,
      :authsource,
      :canonicalizehostname,
      :connect,
      :connecttimeoutms,
      :fsync,
      :gssapiservicename,
      :journal,
      :pool_size,
      :readpreference,
      :replicaset,
      :safe,
      :slaveok,
      :sockettimeoutms,
      :ssl,
      :w,
      :wtimeout,
      :wtimeoutms
    ]

    OPT_VALID = {
      :authmechanism        => lambda { |arg| Mongo::Authentication.validate_mechanism(arg) },
      :authsource           => lambda { |arg| arg.length > 0 },
      :canonicalizehostname => lambda { |arg| ['true', 'false'].include?(arg) },
      :connect              => lambda { |arg| [ 'direct', 'replicaset', 'true', 'false', true, false ].include?(arg) },
      :connecttimeoutms     => lambda { |arg| arg =~ /^\d+$/ },
      :fsync                => lambda { |arg| ['true', 'false'].include?(arg) },
      :gssapiservicename    => lambda { |arg| arg.length > 0 },
      :journal              => lambda { |arg| ['true', 'false'].include?(arg) },
      :pool_size            => lambda { |arg| arg.to_i > 0 },
      :readpreference       => lambda { |arg| READ_PREFERENCES.keys.include?(arg) },
      :replicaset           => lambda { |arg| arg.length > 0 },
      :safe                 => lambda { |arg| ['true', 'false'].include?(arg) },
      :slaveok              => lambda { |arg| ['true', 'false'].include?(arg) },
      :sockettimeoutms      => lambda { |arg| arg =~ /^\d+$/ },
      :ssl                  => lambda { |arg| ['true', 'false'].include?(arg) },
      :w                    => lambda { |arg| arg =~ /^\w+$/ },
      :wtimeout             => lambda { |arg| arg =~ /^\d+$/ },
      :wtimeoutms           => lambda { |arg| arg =~ /^\d+$/ }
     }

    OPT_ERR = {
      :authmechanism        => "must be one of #{Mongo::Authentication::MECHANISMS.join(', ')}",
      :authsource           => "must be a string containing the name of the database being used for authentication",
      :canonicalizehostname => "must be 'true' or 'false'",
      :connect              => "must be 'direct', 'replicaset', 'true', or 'false'",
      :connecttimeoutms     => "must be an integer specifying milliseconds",
      :fsync                => "must be 'true' or 'false'",
      :gssapiservicename    => "must be a string containing the name of the GSSAPI service",
      :journal              => "must be 'true' or 'false'",
      :pool_size            => "must be an integer greater than zero",
      :readpreference       => "must be on of #{READ_PREFERENCES.keys.map(&:inspect).join(",")}",
      :replicaset           => "must be a string containing the name of the replica set to connect to",
      :safe                 => "must be 'true' or 'false'",
      :slaveok              => "must be 'true' or 'false'",
      :settimeoutms         => "must be an integer specifying milliseconds",
      :ssl                  => "must be 'true' or 'false'",
      :w                    => "must be an integer indicating number of nodes to replicate to or a string " +
                               "specifying that replication is required to the majority or nodes with a " +
                               "particilar getLastErrorMode.",
      :wtimeout             => "must be an integer specifying milliseconds",
      :wtimeoutms           => "must be an integer specifying milliseconds"
    }

    OPT_CONV = {
      :authmechanism        => lambda { |arg| arg.upcase },
      :authsource           => lambda { |arg| arg },
      :canonicalizehostname => lambda { |arg| arg == 'true' ? true : false },
      :connect              => lambda { |arg| arg == 'false' ? false : arg }, # convert 'false' to FalseClass
      :connecttimeoutms     => lambda { |arg| arg.to_f / 1000 }, # stored as seconds
      :fsync                => lambda { |arg| arg == 'true' ? true : false },
      :gssapiservicename    => lambda { |arg| arg },
      :journal              => lambda { |arg| arg == 'true' ? true : false },
      :pool_size            => lambda { |arg| arg.to_i },
      :readpreference       => lambda { |arg| READ_PREFERENCES[arg] },
      :replicaset           => lambda { |arg| arg },
      :safe                 => lambda { |arg| arg == 'true' ? true : false },
      :slaveok              => lambda { |arg| arg == 'true' ? true : false },
      :sockettimeoutms      => lambda { |arg| arg.to_f / 1000 }, # stored as seconds
      :ssl                  => lambda { |arg| arg == 'true' ? true : false },
      :w                    => lambda { |arg| Mongo::Support.is_i?(arg) ? arg.to_i : arg.to_sym },
      :wtimeout             => lambda { |arg| arg.to_i },
      :wtimeoutms           => lambda { |arg| arg.to_i }
    }

    OPT_CASE_SENSITIVE = [ :authsource,
                           :gssapiservicename,
                           :replicaset,
                           :w
                         ]

    attr_reader :auths,
                :authmechanism,
                :authsource,
                :canonicalizehostname,
                :connect,
                :connecttimeoutms,
                :db_name,
                :fsync,
                :gssapiservicename,
                :journal,
                :nodes,
                :pool_size,
                :readpreference,
                :replicaset,
                :safe,
                :slaveok,
                :sockettimeoutms,
                :ssl,
                :w,
                :wtimeout,
                :wtimeoutms

    # Parse a MongoDB URI. This method is used by MongoClient.from_uri.
    # Returns an array of nodes and an array of db authorizations, if applicable.
    #
    # @note Passwords can contain any character except for ','
    #
    # @param [String] uri The MongoDB URI string.
    def initialize(uri)
      if uri.start_with?('mongodb://')
        uri = uri[10..-1]
      else
        raise MongoArgumentError, "MongoDB URI must match this spec: #{MONGODB_URI_SPEC}"
      end

      hosts, opts = uri.split('?')
      parse_options(opts)
      parse_hosts(hosts)
      validate_connect
    end

    # Create a Mongo::MongoClient or a Mongo::MongoReplicaSetClient based on the URI.
    #
    # @note Don't confuse this with attribute getter method #connect.
    #
    # @return [MongoClient,MongoReplicaSetClient]
    def connection(extra_opts={}, legacy = false, sharded = false)
      opts = connection_options.merge!(extra_opts)
      if(legacy)
        if replicaset?
          ReplSetConnection.new(node_strings, opts)
        else
          Connection.new(host, port, opts)
        end
      else
        if sharded
          MongoShardedClient.new(node_strings, opts)
        elsif replicaset?
          MongoReplicaSetClient.new(node_strings, opts)
        else
          MongoClient.new(host, port, opts)
        end
      end
    end

    # Whether this represents a replica set.
    # @return [true,false]
    def replicaset?
      replicaset.is_a?(String) || nodes.length > 1
    end

    # Whether to immediately connect to the MongoDB node[s]. Defaults to true.
    # @return [true, false]
    def connect?
      connect != false
    end

    # Whether this represents a direct connection.
    #
    # @note Specifying :connect => 'direct' has no effect... other than to raise an exception if other variables suggest a replicaset.
    #
    # @return [true,false]
    def direct?
      !replicaset?
    end

    # For direct connections, the host of the (only) node.
    # @return [String]
    def host
      nodes[0][0]
    end

    # For direct connections, the port of the (only) node.
    # @return [Integer]
    def port
      nodes[0][1].to_i
    end

    # Options that can be passed to MongoClient.new or MongoReplicaSetClient.new
    # @return [Hash]
    def connection_options
      opts = {}

      if @wtimeout
        warn "Using wtimeout in a URI is deprecated, please use wtimeoutMS. It will be removed in v2.0."
        opts[:wtimeout] = @wtimeout
      end
      opts[:wtimeout] = @wtimeoutms if @wtimeoutms

      opts[:w]     = 1 if @safe
      opts[:w]     = @w if @w
      opts[:j]     = @journal if @journal
      opts[:fsync] = @fsync if @fsync

      opts[:connect_timeout] = @connecttimeoutms if @connecttimeoutms
      opts[:op_timeout]      = @sockettimeoutms if @sockettimeoutms
      opts[:pool_size]       = @pool_size if @pool_size
      opts[:read]            = @readpreference if @readpreference

      if @slaveok && !@readpreference
        unless replicaset?
          opts[:slave_ok] = true
        else
          opts[:read] = :secondary_preferred
        end
      end

      if replicaset.is_a?(String)
        opts[:name] = replicaset
      end

      opts[:db_name] = @db_name if @db_name
      opts[:auths]   = @auths if @auths
      opts[:ssl]     = @ssl if @ssl
      opts[:connect] = connect?

      opts
    end

    def node_strings
      nodes.map { |node| node.join(':') }
    end

    private

    def parse_hosts(uri_without_protocol)
      @nodes = []
      @auths = Set.new

      unless matches = MONGODB_URI_MATCHER.match(uri_without_protocol)
        raise MongoArgumentError,
          "MongoDB URI must match this spec: #{MONGODB_URI_SPEC}"
      end

      user_info = matches[2].split(':') if matches[2]
      host_info = matches[3].split(',')
      @db_name  = matches[8]

      host_info.each do |host|
        if host[0,1] == '['
          host, port = host.split(']:') << MongoClient::DEFAULT_PORT
          host = host.end_with?(']') ? host[1...-1] : host[1..-1]
        else
          host, port = host.split(':') << MongoClient::DEFAULT_PORT
        end
        unless port.to_s =~ /^\d+$/
          raise MongoArgumentError,
            "Invalid port #{port}; port must be specified as digits."
        end
        @nodes << [host, port.to_i]
      end

      if @nodes.empty?
        raise MongoArgumentError,
          "No nodes specified. Please ensure that you've provided at " +
          "least one node."
      end

      # no user info to parse, exit here
      return unless user_info

      # check for url encoding for username and password
      username, password = user_info
      if user_info.size > 2 ||
         (username && username.include?('@')) ||
         (password && password.include?('@'))

        raise MongoArgumentError,
          "The characters ':' and '@' in a username or password " +
          "must be escaped (RFC 2396)."
      end

      # if username exists, proceed adding to auth set
      unless username.nil? || username.empty?
        auth = Authentication.validate_credentials({
          :db_name   => @db_name,
          :username  => URI.unescape(username),
          :password  => password ? URI.unescape(password) : nil,
          :source    => @authsource,
          :mechanism => @authmechanism
        })
        auth[:extra] = @canonicalizehostname ? { :canonicalize_host_name => @canonicalizehostname } : {}
        auth[:extra].merge!(:gssapi_service_name => @gssapiservicename) if @gssapiservicename
        @auths << auth
      end
    end

    # This method uses the lambdas defined in OPT_VALID and OPT_CONV to validate
    # and convert the given options.
    def parse_options(string_opts)
      # initialize instance variables for available options
      OPT_VALID.keys.each { |k| instance_variable_set("@#{k}", nil) }

      string_opts ||= ''

      return if string_opts.empty?

      if string_opts.include?(';') and string_opts.include?('&')
        raise MongoArgumentError, 'must not mix URL separators ; and &'
      end

      opts = CGI.parse(string_opts).inject({}) do |memo, (key, value)|
        value = value.first
        key_sym = key.downcase.to_sym
        memo[key_sym] = OPT_CASE_SENSITIVE.include?(key_sym) ? value.strip : value.strip.downcase
        memo
      end

      opts.each do |key, value|
        if !OPT_ATTRS.include?(key)
          raise MongoArgumentError, "Invalid Mongo URI option #{key}"
        end
        if OPT_VALID[key].call(value)
          instance_variable_set("@#{key}", OPT_CONV[key].call(value))
        else
          raise MongoArgumentError, "Invalid value #{value.inspect} for #{key}: #{OPT_ERR[key]}"
        end
      end
    end

    def validate_connect
      if replicaset? and @connect == 'direct'
        # Make sure the user doesn't specify something contradictory
        raise MongoArgumentError, "connect=direct conflicts with setting a replicaset name"
      end
    end
  end
end

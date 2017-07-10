# Copyright (C) 2014-2017 MongoDB, Inc.
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

RSpec::Matchers.define :have_hosts do |test|

  match do |cl|

    def find_server(client, host)
      client.cluster.instance_variable_get(:@servers).detect do |s|
        s.address.host == host.host
      end
    end

    def match_host?(server, host)
      server.address.host == host.host
    end

    def match_port?(server, host)
      server.address.port == host.port || !host.port
    end

    def match_address_family?(server, host)
      address_family(server) == host.address_family
    end

    def address_family(server)
      server.address.socket(2)
      server.address.instance_variable_get(:@resolver).class
    end

    test.hosts.all? do |host|
      server = find_server(cl, host)
      match_host?(server, host) &&
          match_port?(server, host) if server #&&
          #match_address_family?(server, host) if server
    end

    failure_message do |client|
      "With URI: #{test.uri_string}\n" +
          "Expected that test hosts: #{test.hosts} would match " +
          "client hosts: #{cl.cluster.instance_variable_get(:@servers)}"
    end
  end
end

RSpec::Matchers.define :match_auth do |test|

  def match_database?(client, auth)
    client.options[:database] == auth.database || !auth.database
  end

  def match_password?(client, auth)
    client.options[:password] == auth.password ||
      client.options[:password].nil? && auth.password == ''
  end

  match do |client|
    auth = test.auth
    return true unless auth
    client.options[:user] == auth.username &&
      match_password?(client, auth) &&
        match_database?(client, auth)
  end

  failure_message do |client|
    "With URI: #{test.uri_string}\n" +
        "Expected that test auth: #{test.auth} would match client auth: #{client.options}"
  end
end

RSpec::Matchers.define :match_options do |test|

  match do |client|
    options = test.options
    return true unless options
    options.match?(client.options)
  end

  failure_message do |client|
    "With URI: #{test.uri_string}\n" +
      "Expected that test options: #{test.options.options} would match client options: #{client.options}"
  end
end

module Mongo
  module ConnectionString

    class Spec

      attr_reader :description

      # Instantiate the new spec.
      #
      # @example Create the spec.
      #   Spec.new(file)
      #
      # @param [ String ] file The name of the file.
      #
      # @since 2.0.0
      def initialize(file)
        file = File.new(file)
        @spec = YAML.load(ERB.new(file.read).result)
        file.close
        @description = File.basename(file)
      end

      def tests
        @tests ||= @spec['tests'].collect do |spec|
          Test.new(spec)
        end
      end
    end

    class Test

      attr_reader :description
      attr_reader :uri_string

      def initialize(spec)
        @spec = spec
        @description = @spec['description']
        @uri_string = @spec['uri']
      end

      def valid?
        @spec['valid']
      end

      def warn?
        @spec['warning']
      end

      def hosts
        @hosts ||= @spec['hosts'].collect do |host|
          Host.new(host)
        end
      end

      def options
        @options ||= Options.new(@spec['options']) if @spec['options']
      end

      def client
        @client ||= Mongo::Client.new(@spec['uri'])
      end

      def uri
        @uri ||= Mongo::URI.new(@spec['uri'])
      end

      def auth
        @auth ||= Auth.new(@spec['auth']) if @spec['auth']
      end
    end

    class Host

      MAPPING = {
          'ipv4' => Mongo::Address::IPv4,
          'ipv6' => Mongo::Address::IPv6,
          'unix' => Mongo::Address::Unix
      }

      attr_reader :host
      attr_reader :port

      def initialize(spec)
        @spec = spec
        @host = @spec['host']
        @port = @spec['port']
      end

      def address_family
        MAPPING[@spec['type']]
      end
    end

    class Auth

      attr_reader :username
      attr_reader :password
      attr_reader :database

      def initialize(spec)
        @spec = spec
        @username = @spec['username']
        @password = @spec['password']
        @database = @spec['db']
      end

      def to_s
        "username: #{username}, password: #{password}, database: #{database}"
      end
    end

    class Options

      MAPPINGS = {
          'replicaset' => :replica_set,
          'authmechanism' => :auth_mech
      }

      attr_reader :options

      def initialize(options)
        @options = options
      end

      def match?(opts)
        @options.keys.all? do |k|
          opts[MAPPINGS[k]] == @options[k] ||
              Mongo::URI::AUTH_MECH_MAP[@options[k]] == opts[MAPPINGS[k]]
        end
      end
    end
  end
end

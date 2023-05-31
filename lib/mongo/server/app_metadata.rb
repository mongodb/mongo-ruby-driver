# frozen_string_literal: true

# Copyright (C) 2016-2020 MongoDB Inc.
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

require 'mongo/server/app_metadata/environment'
require 'mongo/server/app_metadata/platform'
require 'mongo/server/app_metadata/truncator'

module Mongo
  class Server
    # Application metadata that is sent to the server during a handshake,
    #   when a new connection is established.
    #
    # @api private
    class AppMetadata
      extend Forwardable

      # The max application name byte size.
      MAX_APP_NAME_SIZE = 128

      # The driver name.
      DRIVER_NAME = 'mongo-ruby-driver'

      # Option keys that affect auth mechanism negotiation.
      AUTH_OPTION_KEYS = %i[ user auth_source auth_mech].freeze

      # Possible connection purposes.
      PURPOSES = %i[ application monitor push_monitor ].freeze

      # Instantiate the new AppMetadata object.
      #
      # @example Instantiate the app metadata.
      #   Mongo::Server::AppMetadata.new(options)
      #
      # @param [ Hash ] options Metadata options.
      # @option options [ String, Symbol ] :app_name Application name that is
      #   printed to the mongod logs upon establishing a connection in server
      #   versions >= 3.4.
      # @option options [ Symbol ] :auth_mech The authentication mechanism to
      #   use. One of :mongodb_cr, :mongodb_x509, :plain, :scram, :scram256
      # @option options [ String ] :auth_source The source to authenticate from.
      # @option options [ Array<String> ] :compressors A list of potential
      #   compressors to use, in order of preference. The driver chooses the
      #   first compressor that is also supported by the server. Currently the
      #   driver only supports 'zstd', 'snappy' and 'zlib'.
      # @option options [ String ] :platform Platform information to include in
      #   the metadata printed to the mongod logs upon establishing a connection
      #   in server versions >= 3.4.
      # @option options [ Symbol ] :purpose The purpose of this connection.
      # @option options [ Hash ] :server_api The requested server API version.
      #   This hash can have the following items:
      #   - *:version* -- string
      #   - *:strict* -- boolean
      #   - *:deprecation_errors* -- boolean
      # @option options [ String ] :user The user name.
      # @option options [ Array<Hash> ] :wrapping_libraries Information about
      #   libraries such as ODMs that are wrapping the driver. Specify the
      #   lower level libraries first. Allowed hash keys: :name, :version,
      #   :platform.
      #
      # @since 2.4.0
      def initialize(options = {})
        @app_name = options[:app_name].to_s if options[:app_name]
        @platform = options[:platform]

        @purpose = check_purpose!(options[:purpose])

        @compressors = options[:compressors] || []
        @wrapping_libraries = options[:wrapping_libraries]
        @server_api = options[:server_api]

        return unless options[:user] && !options[:auth_mech]

        auth_db = options[:auth_source] || 'admin'
        @request_auth_mech = "#{auth_db}.#{options[:user]}"
      end

      # @return [ Symbol ] The purpose of the connection for which this
      #   app metadata is created.
      attr_reader :purpose

      # @return [ String ] The platform information given when the object was
      #   instantiated.
      attr_reader :platform

      # @return [ Hash | nil ] The requested server API version.
      #
      #   Thes hash can have the following items:
      #   - *:version* -- string
      #   - *:strict* -- boolean
      #   - *:deprecation_errors* -- boolean
      attr_reader :server_api

      # @return [ Array<Hash> | nil ] Information about libraries wrapping
      #   the driver.
      attr_reader :wrapping_libraries

      # Get the metadata as BSON::Document to be sent to
      # as part of the handshake. The document should
      # be appended to a suitable handshake command.
      #
      # This method ensures that the metadata are valid.
      #
      # @return [BSON::Document] Valid document for connection's handshake.
      #
      # @raise [ Error::InvalidApplicationName ] When the metadata are invalid.
      def validated_document
        validate!
        document
      end

      # Get BSON::Document to be used as value for `client` key in
      # handshake document.
      #
      # @return [BSON::Document] Document describing client for handshake.
      def client_document
        @client_document ||=
          BSON::Document.new.tap do |doc|
            doc[:application] = { name: @app_name } if @app_name
            doc[:driver] = driver_doc
            doc[:os] = os_doc
            doc[:platform] = platform_string
            env_doc.tap { |env| doc[:env] = env if env }
          end
      end

      private

      # Check whether it is possible to build a valid app metadata document
      # with params provided on intialization.
      #
      # @raise [ Error::InvalidApplicationName ] When the metadata are invalid.
      def validate!
        if @app_name && @app_name.bytesize > MAX_APP_NAME_SIZE
          raise Error::InvalidApplicationName.new(@app_name, MAX_APP_NAME_SIZE)
        end

        true
      end

      # Get the metadata as BSON::Document to be sent to
      # as part of the handshake. The document should
      # be appended to a suitable handshake command.
      #
      # @return [BSON::Document] Document for connection's handshake.
      def document
        @document ||= begin
          client = Truncator.new(client_document).document
          BSON::Document.new(compression: @compressors, client: client).tap do |doc|
            doc[:saslSupportedMechs] = @request_auth_mech if @request_auth_mech
            doc.update(Utils.transform_server_api(@server_api)) if @server_api
          end
        end
      end

      def driver_doc
        names = [ DRIVER_NAME ]
        versions = [ Mongo::VERSION ]
        wrapping_libraries&.each do |library|
          names << (library[:name] || '')
          versions << (library[:version] || '')
        end

        {
          name: names.join('|'),
          version: versions.join('|'),
        }
      end

      def os_doc
        {
          type: type,
          name: name,
          architecture: architecture,
        }
      end

      # Returns the environment doc describing the current FaaS environment.
      #
      # @return [ Hash | nil ] the environment doc (or nil if not in a FaaS
      #   environment).
      def env_doc
        env = Environment.new
        env.faas? ? env.to_h : nil
      end

      def type
        if RbConfig::CONFIG && RbConfig::CONFIG['host_os']
          RbConfig::CONFIG['host_os'].split('_').first[/[a-z]+/i].downcase
        else
          'unknown'
        end
      end

      def name
        RbConfig::CONFIG['host_os']
      end

      def architecture
        RbConfig::CONFIG['target_cpu']
      end

      def platform_string
        Platform.new(self).to_s
      end

      # Verifies that the given purpose is either nil, or is one of the
      # allowed purposes.
      #
      # @param [ String | nil ] purpose The purpose to validate
      #
      # @return [ String | nil ] the {{purpose}} argument
      #
      # @raise [ ArgumentError ] if the purpose is invalid
      def check_purpose!(purpose)
        return purpose unless purpose && !PURPOSES.include?(purpose)

        raise ArgumentError, "Invalid purpose: #{purpose}"
      end
    end
  end
end

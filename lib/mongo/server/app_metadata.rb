# frozen_string_literal: true
# encoding: utf-8

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

module Mongo
  class Server
    # Application metadata that is sent to the server during a handshake,
    #   when a new connection is established.
    #
    # @api private
    #
    # @since 2.4.0
    class AppMetadata
      extend Forwardable

      # The max application metadata document byte size.
      #
      # @since 2.4.0
      MAX_DOCUMENT_SIZE = 512.freeze

      # The max application name byte size.
      #
      # @since 2.4.0
      MAX_APP_NAME_SIZE = 128.freeze

      # The driver name.
      #
      # @since 2.4.0
      DRIVER_NAME = 'mongo-ruby-driver'

      # Option keys that affect auth mechanism negotiation.
      #
      # @api private
      AUTH_OPTION_KEYS = [:user, :auth_source, :auth_mech].freeze

      # Possible connection purposes.
      #
      # @api private
      PURPOSES = %i(application monitor push_monitor).freeze

      # Instantiate the new AppMetadata object.
      #
      # @api private
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
        if @purpose = options[:purpose]
          unless PURPOSES.include?(@purpose)
            raise ArgumentError, "Invalid purpose: #{@purpose}"
          end
        end
        @compressors = options[:compressors] || []
        @wrapping_libraries = options[:wrapping_libraries]
        @server_api = options[:server_api]

        if options[:user] && !options[:auth_mech]
          auth_db = options[:auth_source] || 'admin'
          @request_auth_mech = "#{auth_db}.#{options[:user]}"
        end
      end

      # @return [ Symbol ] The purpose of the connection for which this
      #   app metadata is created.
      #
      # @api private
      attr_reader :purpose

      # @return [ Hash | nil ] The requested server API version.
      #
      #   Thes hash can have the following items:
      #   - *:version* -- string
      #   - *:strict* -- boolean
      #   - *:deprecation_errors* -- boolean
      #
      # @api private
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
      #
      # @api private
      def validated_document
        validate!
        document
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

      # Get BSON::Document to be used as value for `client` key in
      # handshake document.
      #
      # @return [BSON::Document] Document describing client for handshake.
      def full_client_document
        BSON::Document.new.tap do |doc|
          doc[:application] = { name: @app_name } if @app_name
          doc[:driver] = driver_doc
          doc[:os] = os_doc
          doc[:platform] = platform
        end
      end


      # Get the metadata as BSON::Document to be sent to
      # as part of the handshake. The document should
      # be appended to a suitable handshake command.
      #
      # @return [BSON::Document] Document for connection's handshake.
      def document
        @document ||= begin
          client_document = full_client_document
          while client_document.to_bson.to_s.size > MAX_DOCUMENT_SIZE do
            if client_document[:os][:name] || client_document[:os][:architecture]
              client_document[:os].delete(:name)
              client_document[:os].delete(:architecture)
            elsif client_document[:platform]
              client_document.delete(:platform)
            else
              client_document = nil
            end
          end
          document = BSON::Document.new(
            {
              compression: @compressors,
              client: client_document,
            }
          )
          document[:saslSupportedMechs] = @request_auth_mech if @request_auth_mech
          if @server_api
            document.update(
              Utils.transform_server_api(@server_api)
            )
          end
          document
        end
      end

      def driver_doc
        names = [DRIVER_NAME]
        versions = [Mongo::VERSION]
        if wrapping_libraries
          wrapping_libraries.each do |library|
            names << library[:name] || ''
            versions << library[:version] || ''
          end
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
          architecture: architecture
        }
      end

      def type
        (RbConfig::CONFIG && RbConfig::CONFIG['host_os']) ?
          RbConfig::CONFIG['host_os'].split('_').first[/[a-z]+/i].downcase : 'unknown'
      end

      def name
        RbConfig::CONFIG['host_os']
      end

      def architecture
        RbConfig::CONFIG['target_cpu']
      end

      def platform
        if BSON::Environment.jruby?
          ruby_versions = ["JRuby #{JRUBY_VERSION}", "like Ruby #{RUBY_VERSION}"]
          platforms = [RUBY_PLATFORM, "JVM #{java.lang.System.get_property('java.version')}"]
        else
          ruby_versions = ["Ruby #{RUBY_VERSION}"]
          platforms = [RUBY_PLATFORM]
        end
        platforms = [
          @platform,
          *ruby_versions,
          *platforms,
          RbConfig::CONFIG['build'],
        ]
        if @purpose
          platforms << @purpose.to_s[0].upcase
        end
        platform = platforms.compact.join(', ')
        platforms = [platform]
        if wrapping_libraries
          wrapping_libraries.each do |library|
            platforms << library[:platform] || ''
          end
        end
        platforms.join('|')
      end
    end
  end
end

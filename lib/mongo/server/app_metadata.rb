# Copyright (C) 2016-2018 MongoDB, Inc.
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

require 'rbconfig'

module Mongo
  class Server
    # Application metadata that is sent to the server in an ismaster command,
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
      # @ since 2.4.0
      MAX_APP_NAME_SIZE = 128.freeze

      # The driver name.
      #
      # @ since 2.4.0
      DRIVER_NAME = 'mongo-ruby-driver'

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
      #   driver only supports 'zlib'.
      # @option options [ String ] :platform Platform information to include in
      #   the metadata printed to the mongod logs upon establishing a connection
      #   in server versions >= 3.4.
      # @option options [ String ] :user The user name.
      #
      # @since 2.4.0
      def initialize(options)
        @app_name = options[:app_name].to_s if options[:app_name]
        @platform = options[:platform]
        @compressors = options[:compressors] || []

        if options[:user] && !options[:auth_mech]
          auth_db = options[:auth_source] || 'admin'
          @request_auth_mech = "#{auth_db}.#{options[:user]}"
        end
      end

      # Get the bytes of the ismaster message including this metadata.
      #
      # @api private
      #
      # @example Get the ismaster message bytes.
      #   metadata.ismaster_bytes
      #
      # @return [ String ] The raw bytes.
      #
      # @since 2.4.0
      def ismaster_bytes
        @ismaster_bytes ||= validate! && serialize.to_s
      end

      private

      def validate!
        if @app_name && @app_name.bytesize > MAX_APP_NAME_SIZE
          raise Error::InvalidApplicationName.new(@app_name, MAX_APP_NAME_SIZE)
        end
        true
      end

      def full_client_document
        BSON::Document.new.tap do |doc|
          doc[:application] = { name: @app_name } if @app_name
          doc[:driver] = driver_doc
          doc[:os] = os_doc
          doc[:platform] = platform
        end
      end

      def serialize
        Protocol::Query.new(Database::ADMIN, Database::COMMAND, document, :limit => -1).serialize
      end

      def document
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
        document = Server::Monitor::Connection::ISMASTER
        document = document.merge(compression: @compressors)
        document[:client] = client_document
        document[:saslSupportedMechs] = @request_auth_mech if @request_auth_mech
        document
      end

      def driver_doc
        {
          name: DRIVER_NAME,
          version: Mongo::VERSION
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
        [
          @platform,
          RUBY_VERSION,
          RUBY_PLATFORM,
          RbConfig::CONFIG['build']
        ].compact.join(', ')
      end
    end
  end
end

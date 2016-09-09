# Copyright (C) 2014-2016 MongoDB, Inc.
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
  class Cluster

    # Application metadata that is sent to the server in an ismaster command,
    #   when a new connection is established.
    #
    # @since 2.4.0
    class AppMetadata
      extend Forwardable

      # The max application metadata document size.
      #
      # @since 2.4.0
      MAX_DOCUMENT_SIZE = 512.freeze

      # The size of the { ismaster: 1 } key-value pair.
      #
      # @since 2.4.0
      ISMASTER_KEY_VALUE_SIZE = 19.freeze

      # The max application name length.
      #
      # @ since 2.4.0
      MAX_APP_NAME_SIZE = 128.freeze

      # Instantiate the new AppMetadata object.
      #
      # @api private
      #
      # @example Instantiate the app metadata.
      #   Mongo::Cluster.AppMetadata.new(cluster)
      #
      # @param [ Mongo::Cluster ] cluster The cluster.
      #
      # @since 2.4.0
      def initialize(cluster)
        @app_name = cluster.options[:app_name]
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
        if @app_name && @app_name.length > MAX_APP_NAME_SIZE
          raise Error::InvalidApplicationName.new(@app_name)
        end
        true
      end

      def document
        @document ||= Server::Monitor::Connection::ISMASTER.merge(client: client_document)
      end

      def client_document
        @client_document ||= {}.tap do |doc|
          doc[:application] = { name: @app_name } if @app_name
          doc[:driver] = driver_doc
          doc[:os] = os_doc
          doc[:platform] = platform
        end
      end

      def serialize
        Protocol::Query.new(Database::ADMIN,
                            Database::COMMAND,
                            document,
                            :limit => -1).serialize(BSON::ByteBuffer.new,
                                                    MAX_DOCUMENT_SIZE + ISMASTER_KEY_VALUE_SIZE)
      rescue Mongo::Error::MaxBSONSize
        if client_document[:os]
          client_document.delete(:os)
          retry
        elsif client_document[:platform]
          client_document.delete(:platform)
          retry
        else
          Server::Monitor::Connection::ISMASTER_BYTES
        end
      end

      def driver_doc
        {
          name: :'mongo-ruby-driver',
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
          RbConfig::CONFIG['host_os'].split('_').first[/[a-z]+/i].downcase.to_sym : :unknown
      end

      def name
        RbConfig::CONFIG['host_os']
      end

      def architecture
        RbConfig::CONFIG['target_cpu']
      end

      def platform
        [RUBY_VERSION, RUBY_PLATFORM, RbConfig::CONFIG['build']].join(', ')
      end
    end
  end
end

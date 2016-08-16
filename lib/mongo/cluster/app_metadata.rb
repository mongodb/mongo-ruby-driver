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


module Mongo
  class Cluster

    # Application metadata that is sent to the server in an ismaster command,
    #   when a new connection is established.
    #
    # @since 2.4.0
    class AppMetadata
      extend Forwardable

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
        @ismaster_bytes ||= validate! && serialized_ismaster
      end

      private

      def validate!
        if serialized_ismaster.length > 512 || (@app_name && @app_name.length > 128)
          raise Error::InvalidHandshakeDocument.new(@app_name)
        end
        true
      end

      def document
        @document ||= { ismaster: 1 }.tap do |metadata|
          metadata[:application] = { name: @app_name } if @app_name
          metadata[:driver] = driver_doc
          metadata[:os] = os_doc
          metadata[:platform] = RUBY_PLATFORM
        end.freeze
      end

      def serialized_ismaster
        @serialized_ismaster ||= Protocol::Query.new(Database::ADMIN,
                                                     Database::COMMAND,
                                                     document,
                                                     :limit => -1).to_s.freeze
      end

      def driver_doc
        {
          name: 'mongo-ruby-driver',
          version: Mongo::VERSION
        }
      end

      def os_doc
        {
          type: type,
          name: name,
          architecture: architecture,
          version: version
        }
      end

      def type
        case RUBY_PLATFORM
          when /darwin|mac/i
            :macosx
          when /mingw|windows/i
            require 'rbconfig'
            RbConfig::CONFIG['host_os'].split('_').first[/[a-z]+/i].downcase.to_sym
          when /linux/i
            :linux
          when /sunos|solaris/i
            :solaris
          when /bsd/i
            :bsd
        end
      end

      def name
        ''
      end

      def architecture
        ''
      end

      def version
        ''
      end
    end
  end
end

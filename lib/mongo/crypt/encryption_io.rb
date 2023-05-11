# frozen_string_literal: true
# rubocop:todo all

# Copyright (C) 2019-2020 MongoDB Inc.
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
  module Crypt

    # A class that implements I/O methods between the driver and
    # the MongoDB server or mongocryptd.
    #
    # @api private
    class EncryptionIO

      # Timeout used for TLS socket connection, reading, and writing.
      # There is no specific timeout written in the spec. See SPEC-1394
      # for a discussion and updates on what this timeout should be.
      SOCKET_TIMEOUT = 10

      # Creates a new EncryptionIO object with information about how to connect
      # to the key vault.
      #
      # @param [ Mongo::Client ] client The client used to connect to the collection
      #   that stores the encrypted documents, defaults to nil.
      # @param [ Mongo::Client ] mongocryptd_client The client connected to mongocryptd,
      #   defaults to nil.
      # @param [ Mongo::Client ] key_vault_client The client connected to the
      #   key vault collection.
      # @param [ Mongo::Client | nil ] metadata_client The client to be used to
      #   obtain collection metadata.
      # @param [ String ] key_vault_namespace The key vault namespace in the format
      #   db_name.collection_name.
      # @param [ Hash ] mongocryptd_options Options related to mongocryptd.
      #
      # @option mongocryptd_options [ Boolean ] :mongocryptd_bypass_spawn
      # @option mongocryptd_options [ String ] :mongocryptd_spawn_path
      # @option mongocryptd_options [ Array<String> ] :mongocryptd_spawn_args
      #
      # @note When being used for auto encryption, all arguments are required.
      #   When being used for explicit encryption, only the key_vault_namespace
      #   and key_vault_client arguments are required.
      #
      # @note This class expects that the key_vault_client and key_vault_namespace
      #   options are not nil and are in the correct format.
      def initialize(
        client: nil, mongocryptd_client: nil, key_vault_namespace:,
        key_vault_client:, metadata_client:, mongocryptd_options: {}
      )
        validate_key_vault_client!(key_vault_client)
        validate_key_vault_namespace!(key_vault_namespace)

        @client = client
        @mongocryptd_client = mongocryptd_client
        @key_vault_db_name, @key_vault_collection_name = key_vault_namespace.split('.')
        @key_vault_client = key_vault_client
        @metadata_client = metadata_client
        @options = mongocryptd_options
      end

      # Query for keys in the key vault collection using the provided
      # filter
      #
      # @param [ Hash ] filter
      #
      # @return [ Array<BSON::Document> ] The query results
      def find_keys(filter)
        key_vault_collection.find(filter).to_a
      end

      # Insert a document into the key vault collection
      #
      # @param [ Hash ] document
      #
      # @return [ Mongo::Operation::Insert::Result ] The insertion result
      def insert_data_key(document)
        key_vault_collection.insert_one(document)
      end

      # Get collection info for a collection matching the provided filter
      #
      # @param [ Hash ] filter
      #
      # @return [ Hash ] The collection information
      def collection_info(db_name, filter)
        unless @metadata_client
          raise ArgumentError, 'collection_info requires metadata_client to have been passed to the constructor, but it was not'
        end

        @metadata_client.use(db_name).database.list_collections(filter: filter, deserialize_as_bson: true).first
      end

      # Send the command to mongocryptd to be marked with intent-to-encrypt markings
      #
      # @param [ Hash ] cmd
      #
      # @return [ Hash ] The marked command
      def mark_command(cmd)
        unless @mongocryptd_client
          raise ArgumentError, 'mark_command requires mongocryptd_client to have been passed to the constructor, but it was not'
        end

        # Ensure the response from mongocryptd is deserialized with { mode: :bson }
        # to prevent losing type information in commands
        options = { execution_options: { deserialize_as_bson: true } }

        begin
          response = @mongocryptd_client.database.command(cmd, options)
        rescue Error::NoServerAvailable => e
          raise e if @options[:mongocryptd_bypass_spawn]

          spawn_mongocryptd
          response = @mongocryptd_client.database.command(cmd, options)
        end

        return response.first
      end

      # Get information about the remote KMS encryption key and feed it to the the
      # KmsContext object
      #
      # @param [ Mongo::Crypt::KmsContext ] kms_context A KmsContext object
      #   corresponding to one remote KMS data key. Contains information about
      #   the endpoint at which to establish a TLS connection and the message
      #   to send on that connection.
      # @param [ Hash ] tls_options. TLS options to connect to KMS provider.
      #   The options are same as for Mongo::Client.
      def feed_kms(kms_context, tls_options)
        with_ssl_socket(kms_context.endpoint, tls_options) do |ssl_socket|
          Timeout.timeout(SOCKET_TIMEOUT, Error::SocketTimeoutError,
            'Socket write operation timed out'
          ) do
            ssl_socket.syswrite(kms_context.message)
          end

          bytes_needed = kms_context.bytes_needed
          while bytes_needed > 0 do
            bytes = Timeout.timeout(SOCKET_TIMEOUT, Error::SocketTimeoutError,
              'Socket read operation timed out'
            ) do
              ssl_socket.sysread(bytes_needed)
            end

            kms_context.feed(bytes)
            bytes_needed = kms_context.bytes_needed
          end
        end
      end

      # Adds a key_alt_name to the key_alt_names array of the key document
      # in the key vault collection with the given id.
      def add_key_alt_name(id, key_alt_name)
        key_vault_collection.find_one_and_update(
          { _id: id },
          { '$addToSet' => { keyAltNames: key_alt_name } },
        )
      end

      # Removes the key document with the given id
      # from the key vault collection.
      def delete_key(id)
        key_vault_collection.delete_one(_id: id)
      end

      # Finds a single key document with the given id.
      def get_key(id)
        key_vault_collection.find(_id: id).first
      end

      # Returns a key document in the key vault collection with
      # the given key_alt_name.
      def get_key_by_alt_name(key_alt_name)
        key_vault_collection.find(keyAltNames: key_alt_name).first
      end

      # Finds all documents in the key vault collection.
      def get_keys
        key_vault_collection.find
      end

      # Removes a key_alt_name from the key_alt_names array of the key document
      # in the key vault collection with the given id.
      def remove_key_alt_name(id, key_alt_name)
        key_vault_collection.find_one_and_update(
          { _id: id },
          [
            {
              '$set' => {
                keyAltNames: {
                  '$cond' => [
                    { '$eq' => [ '$keyAltNames', [ key_alt_name ] ] },
                    '$$REMOVE',
                    {
                      '$filter' => {
                        input: '$keyAltNames',
                        cond: { '$ne' =>  [ '$$this', key_alt_name ] }
                      }
                    }
                  ]
                }
              }
            }
          ]
        )
      end

      # Apply given requests to the key vault collection using bulk write.
      #
      # @param [ Array<Hash> ] requests The bulk write requests.
      #
      # @return [ BulkWrite::Result ] The result of the operation.
      def update_data_keys(updates)
        key_vault_collection.bulk_write(updates)
      end

      private

      def validate_key_vault_client!(key_vault_client)
        unless key_vault_client
          raise ArgumentError.new('The :key_vault_client option cannot be nil')
        end

        unless key_vault_client.is_a?(Client)
          raise ArgumentError.new(
            'The :key_vault_client option must be an instance of Mongo::Client'
          )
        end
      end

      def validate_key_vault_namespace!(key_vault_namespace)
        unless key_vault_namespace
          raise ArgumentError.new('The :key_vault_namespace option cannot be nil')
        end

        unless key_vault_namespace.split('.').length == 2
          raise ArgumentError.new(
            "#{key_vault_namespace} is an invalid key vault namespace." +
            "The :key_vault_namespace option must be in the format database.collection"
          )
        end
      end

      # Use the provided key vault client and namespace to construct a
      # Mongo::Collection object representing the key vault collection.
      def key_vault_collection
        @key_vault_collection ||= @key_vault_client.with(
          database: @key_vault_db_name,
          read_concern: { level: :majority },
          write_concern: { w: :majority }
        )[@key_vault_collection_name]
      end

      # Spawn a new mongocryptd process using the mongocryptd_spawn_path
      # and mongocryptd_spawn_args passed in through the extra auto
      # encrypt options. Stdout and Stderr of this new process are written
      # to /dev/null.
      #
      # @note To capture the mongocryptd logs, add "--logpath=/path/to/logs"
      #   to auto_encryption_options -> extra_options -> mongocrpytd_spawn_args
      #
      # @return [ Integer ] The process id of the spawned process
      #
      # @raise [ ArgumentError ] Raises an exception if no encryption options
      #   have been provided
      def spawn_mongocryptd
        mongocryptd_spawn_args = @options[:mongocryptd_spawn_args]
        mongocryptd_spawn_path = @options[:mongocryptd_spawn_path]

        unless mongocryptd_spawn_path
          raise ArgumentError.new(
            'Cannot spawn mongocryptd process when no ' +
            ':mongocryptd_spawn_path option is provided'
          )
        end

        if mongocryptd_spawn_path.nil? ||
          mongocryptd_spawn_args.nil? || mongocryptd_spawn_args.empty?
        then
          raise ArgumentError.new(
            'Cannot spawn mongocryptd process when no :mongocryptd_spawn_args ' +
            'option is provided. To start mongocryptd without arguments, pass ' +
            '"--" for :mongocryptd_spawn_args'
          )
        end

        begin
          Process.spawn(
            mongocryptd_spawn_path,
            *mongocryptd_spawn_args,
            [:out, :err]=>'/dev/null'
          )
        rescue Errno::ENOENT => e
          raise Error::MongocryptdSpawnError.new(
            "Failed to spawn mongocryptd at the path \"#{mongocryptd_spawn_path}\" " +
            "with arguments #{mongocryptd_spawn_args}. Received error " +
            "#{e.class}: \"#{e.message}\""
          )
        end
      end

      # Provide a TLS socket to be used for KMS calls in a block API
      #
      # @param [ String ] endpoint The URI at which to connect the TLS socket.
      # @param [ Hash ] tls_options. TLS options to connect to KMS provider.
      #   The options are same as for Mongo::Client.
      # @yieldparam [ OpenSSL::SSL::SSLSocket ] ssl_socket Yields a TLS socket
      #   connected to the specified endpoint.
      #
      # @raise [ Mongo::Error::KmsError ] If the socket times out or raises
      #   an exception
      #
      # @note The socket is always closed when the provided block has finished
      #   executing
      def with_ssl_socket(endpoint, tls_options)
        address = begin
          host, port = endpoint.split(':')
          port ||= 443 # All supported KMS APIs use this port by default.
          Address.new([host, port].join(':'))
        end
        mongo_socket = address.socket(
          SOCKET_TIMEOUT,
          tls_options.merge(ssl: true)
        )
        yield(mongo_socket.socket)
      rescue => e
        raise Error::KmsError, "Error when connecting to KMS provider: #{e.class}: #{e.message}"
      ensure
        mongo_socket&.close
      end
    end
  end
end

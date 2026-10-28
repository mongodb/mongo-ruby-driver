# frozen_string_literal: true

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
      # @param [ Proc | nil ] kms_connect_callback Optional callable that
      #   establishes the TCP connection to a KMS provider. It is invoked with
      #   the host, port, and operation timeout in milliseconds (or nil), and
      #   must return an IO connected to the host.
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
        key_vault_namespace:, key_vault_client:, metadata_client:, client: nil, mongocryptd_client: nil, mongocryptd_options: {}, kms_connect_callback: nil
      )
        validate_key_vault_client!(key_vault_client)
        validate_key_vault_namespace!(key_vault_namespace)
        if kms_connect_callback && !kms_connect_callback.respond_to?(:call)
          raise ArgumentError, 'kms_connect_callback must respond to :call'
        end

        @client = client
        @mongocryptd_client = mongocryptd_client
        @key_vault_db_name, @key_vault_collection_name = key_vault_namespace.split('.')
        @key_vault_client = key_vault_client
        @metadata_client = metadata_client
        @options = mongocryptd_options
        @kms_connect_callback = kms_connect_callback
      end

      # Query for keys in the key vault collection using the provided
      # filter
      #
      # @param [ Hash ] filter
      # @param [ Integer ] :timeout_ms The operation timeout in milliseconds.
      #    Must be a non-negative integer. An explicit value of 0 means infinite.
      #    The default value is unset which means the feature is not enabled.
      #
      # @return [ Array<BSON::Document> ] The query results
      def find_keys(filter, timeout_ms: nil)
        key_vault_collection.find(filter, timeout_ms: timeout_ms).to_a
      end

      # Insert a document into the key vault collection
      #
      # @param [ Hash ] document
      # @param [ Integer ] :timeout_ms The operation timeout in milliseconds.
      #    Must be a non-negative integer. An explicit value of 0 means infinite.
      #    The default value is unset which means the feature is not enabled.
      #
      # @return [ Mongo::Operation::Insert::Result ] The insertion result
      def insert_data_key(document, timeout_ms: nil)
        key_vault_collection.insert_one(document, timeout_ms: timeout_ms)
      end

      # Get collection info for a collection matching the provided filter
      #
      # @param [ Hash ] filter
      # @param [ Integer ] :timeout_ms The operation timeout in milliseconds.
      #    Must be a non-negative integer. An explicit value of 0 means infinite.
      #    The default value is unset which means the feature is not enabled.
      #
      # @return [ Array<BSON::Document> ] The collection information documents
      def collection_info(db_name, filter, timeout_ms: nil)
        unless @metadata_client
          raise ArgumentError,
                'collection_info requires metadata_client to have been passed to the constructor, but it was not'
        end

        @metadata_client
          .use(db_name)
          .database
          .list_collections(filter: filter, deserialize_as_bson: true, timeout_ms: timeout_ms)
          .to_a
      end

      # Send the command to mongocryptd to be marked with intent-to-encrypt markings
      #
      # @param [ Hash ] cmd
      # @param [ String | nil ] :db_name The database against which the command
      #   is being run. When provided, the command is sent to mongocryptd using
      #   this database so that the namespace in the command matches the namespace
      #   in encryptionInformation.
      # @param [ Integer ] :timeout_ms The operation timeout in milliseconds.
      #    Must be a non-negative integer. An explicit value of 0 means infinite.
      #    The default value is unset which means the feature is not enabled.
      #
      # @return [ Hash ] The marked command
      def mark_command(cmd, db_name: nil, timeout_ms: nil)
        unless @mongocryptd_client
          raise ArgumentError,
                'mark_command requires mongocryptd_client to have been passed to the constructor, but it was not'
        end

        # Ensure the response from mongocryptd is deserialized with { mode: :bson }
        # to prevent losing type information in commands
        options = {
          execution_options: { deserialize_as_bson: true },
          timeout_ms: timeout_ms
        }

        mongocryptd = db_name ? @mongocryptd_client.use(db_name) : @mongocryptd_client

        begin
          response = mongocryptd.database.command(cmd, options)
        rescue Error::NoServerAvailable => e
          raise e if @options[:mongocryptd_bypass_spawn]

          spawn_mongocryptd
          response = mongocryptd.database.command(cmd, options)
        end

        response.first
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
      # @param [ Integer ] :timeout_ms The operation timeout in milliseconds.
      #    Must be a non-negative integer. An explicit value of 0 means infinite.
      #    The default value is unset which means the feature is not enabled.
      def feed_kms(kms_context, tls_options, timeout_ms: nil)
        # When a budget is in effect the TLS handshake, the request write, and
        # every read chunk share a single deadline, so a slow, chunked response
        # cannot run for a multiple of the budget. Without a budget each phase
        # keeps the fixed SOCKET_TIMEOUT fallback. Timeout.timeout takes
        # seconds, but timeout_ms is in milliseconds.
        deadline = Utils.monotonic_time + (timeout_ms / 1_000.0) if timeout_ms
        # Use the CSOT-appropriate error class when a timeout budget is
        # in effect, otherwise preserve the historical SocketTimeoutError.
        timeout_error = timeout_ms ? Error::TimeoutError : Error::SocketTimeoutError

        with_ssl_socket(kms_context.endpoint, tls_options, timeout_ms: timeout_ms) do |ssl_socket|
          Timeout.timeout(remaining_timeout(deadline), timeout_error,
                          'Socket write operation timed out') do
            ssl_socket.syswrite(kms_context.message)
          end

          bytes_needed = kms_context.bytes_needed
          while bytes_needed > 0
            bytes = Timeout.timeout(remaining_timeout(deadline), timeout_error,
                                    'Socket read operation timed out') do
              ssl_socket.sysread(bytes_needed)
            end

            kms_context.feed(bytes)
            bytes_needed = kms_context.bytes_needed
          end
        end
      end

      # Adds a key_alt_name to the key_alt_names array of the key document
      # in the key vault collection with the given id.
      def add_key_alt_name(id, key_alt_name, timeout_ms: nil)
        key_vault_collection.find_one_and_update(
          { _id: id },
          { '$addToSet' => { keyAltNames: key_alt_name } },
          timeout_ms: timeout_ms
        )
      end

      # Removes the key document with the given id
      # from the key vault collection.
      def delete_key(id, timeout_ms: nil)
        key_vault_collection.delete_one(_id: id, timeout_ms: timeout_ms)
      end

      # Finds a single key document with the given id.
      def get_key(id, timeout_ms: nil)
        key_vault_collection.find(_id: id, timeout_ms: timeout_ms).first
      end

      # Returns a key document in the key vault collection with
      # the given key_alt_name.
      def get_key_by_alt_name(key_alt_name, timeout_ms: nil)
        key_vault_collection.find(keyAltNames: key_alt_name, timeout_ms: timeout_ms).first
      end

      # Finds all documents in the key vault collection.
      def get_keys(timeout_ms: nil)
        key_vault_collection.find(nil, timeout_ms: timeout_ms)
      end

      # Removes a key_alt_name from the key_alt_names array of the key document
      # in the key vault collection with the given id.
      def remove_key_alt_name(id, key_alt_name, timeout_ms: nil)
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
                        cond: { '$ne' => [ '$$this', key_alt_name ] }
                      }
                    }
                  ]
                }
              }
            }
          ],
          timeout_ms: timeout_ms
        )
      end

      # Apply given requests to the key vault collection using bulk write.
      #
      # @param [ Array<Hash> ] requests The bulk write requests.
      #
      # @return [ BulkWrite::Result ] The result of the operation.
      def update_data_keys(updates, timeout_ms: nil)
        key_vault_collection.bulk_write(updates, timeout_ms: timeout_ms)
      end

      private

      # The time left, in seconds, before the shared KMS exchange deadline.
      # When no operation budget is in effect there is no deadline, so each
      # phase falls back to the fixed SOCKET_TIMEOUT.
      #
      # @param [ Float, nil ] deadline The monotonic time the exchange must
      #   finish by, or nil when no budget is set.
      #
      # @return [ Float ] The number of seconds left before the deadline.
      #
      # @raise [ Error::TimeoutError ] If the deadline has already passed.
      def remaining_timeout(deadline)
        return SOCKET_TIMEOUT if deadline.nil?

        remaining = deadline - Utils.monotonic_time
        raise Error::TimeoutError, 'KMS request timed out' if remaining <= 0

        remaining
      end

      def validate_key_vault_client!(key_vault_client)
        raise ArgumentError.new('The :key_vault_client option cannot be nil') unless key_vault_client

        return if key_vault_client.is_a?(Client)

        raise ArgumentError.new(
          'The :key_vault_client option must be an instance of Mongo::Client'
        )
      end

      def validate_key_vault_namespace!(key_vault_namespace)
        raise ArgumentError.new('The :key_vault_namespace option cannot be nil') unless key_vault_namespace

        return if key_vault_namespace.split('.').length == 2

        raise ArgumentError.new(
          "#{key_vault_namespace} is an invalid key vault namespace." +
          'The :key_vault_namespace option must be in the format database.collection'
        )
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
            %i[out err] => '/dev/null'
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
      def with_ssl_socket(endpoint, tls_options, timeout_ms: nil)
        csot = !timeout_ms.nil?
        host, port = endpoint.split(':')
        port = (port || 443).to_i

        mongo_socket =
          if @kms_connect_callback
            connected_socket = @kms_connect_callback.call(host, port, timeout_ms)
            unless connected_socket.is_a?(IO) || connected_socket.respond_to?(:to_io)
              raise Error::KmsError.new(
                'kms_connect_callback must return an IO connected to the KMS provider ' +
                "at #{host}:#{port}, but it returned #{connected_socket.inspect}"
              )
            end

            socket_options = { ssl: true, csot: csot, connected_socket: connected_socket }
            socket_options[:connect_timeout] =
              if csot
                timeout_ms / 1_000.0
              else
                Server::CONNECT_TIMEOUT
              end
            Mongo::Socket::SSL.new(
              host, port, host, SOCKET_TIMEOUT, ::Socket::AF_UNSPEC,
              tls_options.merge(socket_options)
            )
          else
            address = Address.new([ host, port ].join(':'))
            socket_options = { ssl: true, csot: csot }.tap do |opts|
              opts[:connect_timeout] = (timeout_ms / 1_000.0) if csot
            end
            address.socket(SOCKET_TIMEOUT, tls_options.merge(socket_options))
          end

        yield(mongo_socket.socket)
      rescue Error::KmsError
        raise
      rescue StandardError => e
        raise Error::KmsError.new("Error when connecting to KMS provider: #{e.class}: #{e.message}",
                                  network_error: true)
      ensure
        mongo_socket&.close
      end
    end
  end
end

# Copyright (C) 2019 MongoDB, Inc.
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
      # Creates a new EncryptionIO object with information about how to connect
      # to the key vault.
      #
      # @param [ Mongo::Client ] client: The client used to connect to the collection
      #   that stores the encrypted documents, defaults to nil
      # @param [ Mongo::Client ] mongocryptd_client: The client connected to mongocryptd,
      #   defaults to nil
      # @param [ Mongo::Collection ] key_vault_collection: The Collection object
      #   representing the database collection storing the encryption data keys
      #
      # @note This class expects that the key_vault_client and key_vault_namespace
      #   options are not nil and are in the correct format
      def initialize(client: nil, mongocryptd_client: nil, key_vault_collection:)
        @client = client
        @mongocryptd_client = mongocryptd_client
        @key_vault_collection = key_vault_collection
      end

      # Query for keys in the key vault collection using the provided
      # filter
      #
      # @param [ Hash ] filter
      #
      # @return [ Array<BSON::Document> ] The query results
      def find_keys(filter)
        @key_vault_collection.find(filter).to_a
      end

      # Insert a document into the key vault collection
      #
      # @param [ Hash ] document
      #
      # @return [ Mongo::Operation::Insert::Result ] The insertion result
      def insert(document)
        @key_vault_collection.insert_one(document)
      end

      # Get collection info for a collection matching the provided filter
      #
      # @param [ Hash ] filter
      #
      # @return [ Hash ] The collection information
      def collection_info(filter)
        result = @client.database.list_collections

        name = filter['name']
        result.find { |r| r['name'] == name }
      end

      # Send the command to mongocryptd to be marked with intent-to-encrypt markings
      #
      # @param [ Hash ] cmd
      #
      # @return [ Hash ] The marked command
      def mark_command(cmd)
        begin
          response = @mongocryptd_client.database.command(cmd)
        rescue Error::NoServerAvailable => e
          raise e if @client.encryption_options[:mongocryptd_bypass_spawn]

          @client.spawn_mongocryptd
          response = @mongocryptd_client.database.command(cmd)
        end

        return response.first
      end

      # Get information about the AWS encryption key and feed it to the the
      # KMSContext object
      #
      # @param [ Mongo::Crypt::KMSContext ] kms_context A KMSContext object
      #   corresponding to one AWS KMS data key. Contains information about
      #   the endpoint at which to establish a TLS connection and the message
      #   to send on that connection.
      def feed_kms(kms_context)
        endpoint = kms_context.endpoint
        message = kms_context.message

        socket_timeout = 10

        host, port = endpoint.split(':')

        # TODO: do some host/port testing

        ssl_socket = Socket::SSL.new(host, port, host, socket_timeout, Socket::PF_INET)
        ssl_socket.write(message)

        num_bytes_needed = kms_context.bytes_needed

        while num_bytes_needed > 0
          bytes = []
          while !ssl_socket.eof?
            bytes << ssl_socket.readbyte
          end

          kms_context.feed(bytes.pack('C*'))
          num_bytes_needed = kms_context.bytes_needed
        end
      end
    end
  end
end

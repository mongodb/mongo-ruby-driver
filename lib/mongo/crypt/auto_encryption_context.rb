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

    # A Context object initialized for auto encryption
    #
    # @api private
    class AutoEncryptionContext < Context

      # Create a new AutoEncryptionContext object
      #
      # @param [ Mongo::Crypt::Handle ] mongocrypt a Handle that
      #   wraps a mongocrypt_t object used to create a new mongocrypt_ctx_t
      # @param [ ClientEncryption::IO ] io A instance of the IO class
      #   that implements driver I/O methods required to run the
      #   state machine
      # @param [ String ] db_name The name of the database against which
      #   the command is being made
      # @param [ Hash ] command The command to be encrypted
      def initialize(mongocrypt, io, db_name, command)
        super(mongocrypt, io)

        @db_name = db_name
        @command = command

        # Initialize the ctx object for auto encryption
        Binding.ctx_encrypt_init(self, @db_name, @command)
      end
    end
  end
end

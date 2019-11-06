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

    # TODO:
    class ExplicitDecryptionContext < Context

      # TODO: documentation
      def initialize(mongocrypt, value, io)

        super(mongocrypt)

        @value = value
        @io = io

        begin
          initialize_ctx
        rescue => e
          # Setting options on or initializing the context could raise errors.
          # Make sure the reference to the underlying mongocrypt_ctx_t is destroyed
          # before passing those errors along.
          self.close
          raise e
        end
      end

      # Convenient API for using context object without having
      # to perform cleanup.
      # TODO: documentation
      def self.with_context(mongocrypt, value, io)
        context = self.new(mongocrypt, value, io)
        begin
          yield(context)
        ensure
          context.close
        end
      end

      private

      # TODO: documentation
      def initialize_ctx
        Binary.with_binary(@value.data) do |binary|
          success = Binding.mongocrypt_ctx_explicit_decrypt_init(@ctx, binary.ref)
          raise_from_status unless success
        end
      end
    end
  end
end

# Copyright (C) 2015-2017 MongoDB, Inc.
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
  class Error

    # This exception is raised when the server nonce returned does not
    # match the client nonce sent to it.
    #
    # @since 2.0.0
    class InvalidNonce < Error

      # @return [ String ] nonce The client nonce.
      attr_reader :nonce

      # @return [ String ] rnonce The server nonce.
      attr_reader :rnonce

      # Instantiate the new exception.
      #
      # @example Create the exception.
      #   InvalidNonce.new(nonce, rnonce)
      #
      # @param [ String ] nonce The client nonce.
      # @param [ String ] rnonce The server nonce.
      #
      # @since 2.0.0
      def initialize(nonce, rnonce)
        @nonce = nonce
        @rnonce = rnonce
        super("Expected server rnonce '#{rnonce}' to start with client nonce '#{nonce}'.")
      end
    end
  end
end

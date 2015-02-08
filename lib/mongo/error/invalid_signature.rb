# Copyright (C) 2015 MongoDB, Inc.
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

    # This exception is raised when the server verifier does not match the
    # expected signature on the client.
    #
    # @since 2.0.0
    class InvalidSignature < Error

      # @return [ String ] verifier The server verifier string.
      attr_reader :verifier

      # @return [ String ] server_signature The expected server signature.
      attr_reader :server_signature

      # Create the new exception.
      #
      # @example Create the new exception.
      #   InvalidSignature.new(verifier, server_signature)
      #
      # @param [ String ] verifier The verifier returned from the server.
      # @param [ String ] server_signature The expected value from the
      #   server.
      #
      # @since 2.0.0
      def initialize(verifier, server_signature)
        @verifier = verifier
        @server_signature = server_signature
        super("Expected server verifier '#{verifier}' to match '#{server_signature}'.")
      end
    end
  end
end

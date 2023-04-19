# frozen_string_literal: true
# rubocop:todo all

# Copyright (C) 2014-2020 MongoDB Inc.
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

    # Raised if the file md5 and server md5 do not match when acknowledging
    # GridFS writes.
    #
    # @since 2.0.0
    class InvalidFile < Error

      # Create the new exception.
      #
      # @example Create the new exception.
      #   Mongo::Error::InvalidFile.new(file_md5, server_md5)
      #
      # @param [ String ] client_md5 The client side file md5.
      # @param [ String ] server_md5 The server side file md5.
      #
      # @since 2.0.0
      def initialize(client_md5, server_md5)
        super("File MD5 on client side is #{client_md5} but the server reported #{server_md5}.")
      end
    end
  end
end

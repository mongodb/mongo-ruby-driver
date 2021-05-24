# frozen_string_literal: true
# encoding: utf-8

# Copyright (C) 2020 MongoDB Inc.
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
  class Server
    class PushMonitor

      # @api private
      class Connection < Server::Monitor::Connection

        def socket_timeout
          options[:socket_timeout]
        end

        # Build a document that should be used for connection check.
        #
        # @return [BSON::Document] Document that should be sent to a server
        #     as part of the handshake.
        #
        # @api private
        def check_document
          if @app_metadata.server_api && @app_metadata.server_api[:version]
            HELLO_DOC
          else
            LEGACY_HELLO_DOC
          end
        end
      end
    end
  end
end

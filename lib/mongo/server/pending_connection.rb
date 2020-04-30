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
  class Server

    # This class encapsulates connections during handshake and authentication.
    #
    # @api private
    class PendingConnection < ConnectionBase
      extend Forwardable

      def initialize(socket, server, monitoring, options = {})
        @socket = socket
        @options = options
        @server = server
        @monitoring = monitoring
        @id = options[:id]
      end

      # @return [ Integer ] The ID for the connection. This is the same ID
      #   as that of the regular Connection object for which this
      #   PendingConnection instance was created.
      attr_reader :id

      # @return [ Server::Description ] The server description calculated from
      #   ismaster response for this particular connection.
      def handshake!
        unless socket
          raise Error::HandshakeError, "Cannot handshake because there is no usable socket (for #{address})"
        end

        response = average_rtt = nil
        @server.handle_handshake_failure! do
          begin
            response, exc, rtt, average_rtt =
              @server.round_trip_time_averager.measure do
                add_server_diagnostics do
                  socket.write(app_metadata.ismaster_bytes)
                  Protocol::Message.deserialize(socket, Protocol::Message::MAX_MESSAGE_SIZE).documents[0]
                end
              end

            if exc
              raise exc
            end
          rescue => e
            log_warn("Failed to handshake with #{address}: #{e.class}: #{e}:\n#{e.backtrace[0..5].join("\n")}")
            raise
          end
        end

        post_handshake(response, average_rtt)
      end

      def authenticate!
        if options[:user] || options[:auth_mech]
          @server.handle_auth_failure! do
            begin
              Auth.get(user).login(self)
            rescue => e
              log_warn("Failed to handshake with #{address}: #{e.class}: #{e}:\n#{e.backtrace[0..5].join("\n")}")
              raise
            end
          end
        end
      end

      def ensure_connected
        yield @socket
      end

      private

      # This is a separate method to keep the nesting level down.
      #
      # @return [ Server::Description ] The server description calculated from
      #   ismaster response for this particular connection.
      def post_handshake(response, average_rtt)
        if response["ok"] == 1
          # Auth mechanism is entirely dependent on the contents of
          # ismaster response *for this connection*.
          # Ismaster received by the monitoring connection should advertise
          # the same wire protocol, but if it doesn't, we use whatever
          # the monitoring connection advertised for filling out the
          # server description and whatever the non-monitoring connection
          # (that's this one) advertised for performing auth on that
          # connection.
          @sasl_supported_mechanisms = response['saslSupportedMechs']
          set_compressor!(response)
        else
          @sasl_supported_mechanisms = nil
        end

        @description = Description.new(address, response, average_rtt).tap do |new_description|
          @server.cluster.run_sdam_flow(@server.description, new_description)
        end
      end

      def user
        @user ||= begin
          user_options = Options::Redacted.new(:auth_mech => default_mechanism).merge(options)
          if user_options[:auth_mech] == :mongodb_x509
            user_options[:auth_source] = '$external'
          end
          Auth::User.new(user_options)
        end
      end

      def default_mechanism
        if description.features.scram_sha_1_enabled?
          if @sasl_supported_mechanisms&.include?('SCRAM-SHA-256')
            :scram256
          else
            :scram
          end
        else
          :mongodb_cr
        end
      end
    end
  end
end

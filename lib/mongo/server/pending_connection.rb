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

      def handshake_and_authenticate!
        speculative_auth_doc = nil
        if options[:user] || options[:auth_mech]
          # To create an Auth instance, we need to specify the mechanism,
          # but at this point we don't know the mechanism that ultimately
          # will be used (since this depends on the data returned by
          # the handshake, specifically server version).
          # However, we know that only 4.4+ servers support speculative
          # authentication, and those servers also generally support
          # SCRAM-SHA-256. We expect that user accounts created for 4.4+
          # servers would generally allow SCRAM-SHA-256 authentication;
          # user accounts migrated from pre-4.4 servers may only allow
          # SCRAM-SHA-1. The use of SCRAM-SHA-256 by default is thus
          # sensible, and it is also mandated by the speculative auth spec.
          # If no mechanism was specified and we are talking to a 3.0+
          # server, we'll send speculative auth document, the server will
          # ignore it and we'll perform authentication using explicit
          # command after having defaulted the mechanism later to CR.
          # If no mechanism was specified and we are talking to a 4.4+
          # server and the user account doesn't allow SCRAM-SHA-256, we will
          # authenticate in a separate command with SCRAM-SHA-1 after
          # going through SCRAM mechanism negotiation.
          default_options = Options::Redacted.new(:auth_mech => :scram256)
          speculative_auth_user = Auth::User.new(default_options.merge(options))
          speculative_auth = Auth.get(speculative_auth_user, self)
          speculative_auth_doc = speculative_auth.conversation.speculative_auth_document
        end

        result = handshake!(speculative_auth_doc: speculative_auth_doc)

        if description.unknown?
          raise Error::InternalDriverError, "Connection description cannot be unknown after successful handshake: #{description.inspect}"
        end

        begin
          if speculative_auth_doc && (speculative_auth_result = result['speculativeAuthenticate'])
            unless description.features.scram_sha_1_enabled?
              raise Error::InvalidServerAuthResponse, "Speculative auth succeeded on a pre-3.0 server"
            end
            case speculative_auth_user.mechanism
            when :mongodb_x509
              # Done
            # We default auth mechanism to scram256, but if user specified
            # scram explicitly we may be able to authenticate speculatively
            # with scram.
            when :scram, :scram256
              authenticate!(
                speculative_auth_client_nonce: speculative_auth.conversation.client_nonce,
                speculative_auth_mech: speculative_auth_user.mechanism,
                speculative_auth_result: speculative_auth_result,
              )
            else
              raise Error::InternalDriverError, "Speculative auth unexpectedly succeeded for mechanism #{speculative_auth_user.mechanism.inspect}"
            end
          elsif !description.arbiter?
            authenticate!
          end
        rescue Mongo::Error, Mongo::Error::AuthError => exc
          exc.service_id = service_id
          raise
        end

        if description.unknown?
          raise Error::InternalDriverError, "Connection description cannot be unknown after successful authentication: #{description.inspect}"
        end

        if server.load_balancer? && !description.mongos?
          raise Error::BadLoadBalancerTarget, "Load-balanced operation requires being connected a mongos, but the server at #{address.seed} reported itself as #{description.server_type.to_s.gsub('_', ' ')}"
        end
      end

      private

      # @param [ BSON::Document | nil ] speculative_auth_doc The document to
      #   provide in speculativeAuthenticate field of handshake command.
      #
      # @return [ BSON::Document ] The document of the handshake response for
      #   this particular connection.
      def handshake!(speculative_auth_doc: nil)
        unless socket
          raise Error::InternalDriverError, "Cannot handshake because there is no usable socket (for #{address})"
        end

        hello_command = handshake_command(
          handshake_document(
            app_metadata,
            speculative_auth_doc: speculative_auth_doc,
            load_balancer: server.load_balancer?,
            server_api: options[:server_api]
          )
        )
        doc = nil
        @server.handle_handshake_failure! do
          begin
            response = @server.round_trip_time_averager.measure do
              add_server_diagnostics do
                socket.write(hello_command.serialize.to_s)
                Protocol::Message.deserialize(socket, Protocol::Message::MAX_MESSAGE_SIZE)
              end
            end
            result = Operation::Result.new([response])
            result.validate!
            doc = result.documents.first
          rescue => exc
            msg = "Failed to handshake with #{address}"
            Utils.warn_bg_exception(msg, exc,
              logger: options[:logger],
              log_prefix: options[:log_prefix],
              bg_error_backtrace: options[:bg_error_backtrace],
            )
            raise
          end
        end

        if @server.force_load_balancer?
          doc['serviceId'] ||= "fake:#{rand(2**32-1)+1}"
        end

        post_handshake(doc, @server.round_trip_time_averager.average_round_trip_time)

        doc
      end

      # @param [ String | nil ] speculative_auth_client_nonce The client
      #   nonce used in speculative auth on this connection that
      #   produced the specified speculative auth result.
      # @param [ Symbol | nil ] speculative_auth_mech Auth mechanism used
      #   for speculative auth, if speculative auth succeeded. If speculative
      #   auth was not performed or it failed, this must be nil.
      # @param [ BSON::Document | nil ] speculative_auth_result The
      #   value of speculativeAuthenticate field of hello response of
      #   the handshake on this connection.
      def authenticate!(
        speculative_auth_client_nonce: nil,
        speculative_auth_mech: nil,
        speculative_auth_result: nil
      )
        if options[:user] || options[:auth_mech]
          @server.handle_auth_failure! do
            begin
              auth = Auth.get(
                resolved_user(speculative_auth_mech: speculative_auth_mech),
                self,
                speculative_auth_client_nonce: speculative_auth_client_nonce,
                speculative_auth_result: speculative_auth_result,
              )
              auth.login
            rescue => exc
              msg = "Failed to authenticate to #{address}"
              Utils.warn_bg_exception(msg, exc,
                logger: options[:logger],
                log_prefix: options[:log_prefix],
                bg_error_backtrace: options[:bg_error_backtrace],
              )
              raise
            end
          end
        end
      end

      def ensure_connected
        yield @socket
      end

      # This is a separate method to keep the nesting level down.
      #
      # @return [ Server::Description ] The server description calculated from
      #   the handshake response for this particular connection.
      def post_handshake(response, average_rtt)
        if response["ok"] == 1
          # Auth mechanism is entirely dependent on the contents of
          # hello response *for this connection*.
          # Hello received by the monitoring connection should advertise
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

        @description = Description.new(
          address, response,
          average_round_trip_time: average_rtt,
          load_balancer: server.load_balancer?,
          force_load_balancer: options[:connect] == :load_balanced,
        ).tap do |new_description|
          @server.cluster.run_sdam_flow(@server.description, new_description)
        end
      end

      # The user as going to be used for authentication. This user has the
      # auth mechanism set and, if necessary, auth source.
      #
      # @param [ Symbol | nil ] speculative_auth_mech Auth mechanism used
      #   for speculative auth, if speculative auth succeeded. If speculative
      #   auth was not performed or it failed, this must be nil.
      #
      # @return [ Auth::User ] The resolved user.
      def resolved_user(speculative_auth_mech: nil)
        @resolved_user ||= begin
          unless options[:user] || options[:auth_mech]
            raise Mongo::Error, 'No authentication information specified in the client'
          end

          user_options = Options::Redacted.new(
            # When speculative auth is performed, we always use SCRAM-SHA-256.
            # At the same time we perform SCRAM mechanism negotiation in the
            # hello request.
            # If the credentials we are trying to authenticate with do not
            # map to an existing user, SCRAM mechanism negotiation will not
            # return anything which would cause the driver to use
            # SCRAM-SHA-1. However, on 4.4+ servers speculative auth would
            # succeed (technically just the first round-trip, not the entire
            # authentication flow) and we would be continuing it here;
            # in this case, we must use SCRAM-SHA-256 as the mechanism since
            # that is what the conversation was started with, even though
            # SCRAM mechanism negotiation did not return SCRAM-SHA-256 as a
            # valid mechanism to use for these credentials.
            :auth_mech => speculative_auth_mech || default_mechanism,
          ).merge(options)
          if user_options[:auth_mech] == :mongodb_x509
            user_options[:auth_source] = '$external'
          end
          Auth::User.new(user_options)
        end
      end

      def default_mechanism
        if description.nil?
          raise Mongo::Error, 'Trying to query default mechanism when handshake has not completed'
        end

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

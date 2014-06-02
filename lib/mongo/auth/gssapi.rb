# Copyright (C) 2009-2014 MongoDB, Inc.
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
  module Auth

    # An authenticator for GSSAPI authentication against MongoDB.
    #
    # @since 2.0.0
    class GSSAPI
      include Authenticatable

      # Log in with GSSAPI authentication.
      #
      # @param [ Mongo::Server ] server The server on which to authenticate.
      #
      # @since 2.0.0
      def login(server)
        @authenticator = gssapi_authenticator(server)
        response = server.dispatch(start_message)
        until response['done'] do
          response = server.dispatch(continue_message(response))
        end
      end

      private

      # Validate credentials for GSSAPI authentication
      #
      # @since 2.0.0
      def validate_credentials
        unless RUBY_PLATFORM =~ /java/
          raise NotImplementedError, 
          "The GSSAPI authentication mechanism is only supported for JRuby."
        end
      end

      # Return a GSSAPI authenticator.
      #
      # @return [ GSSAPIAuthanticator ] the authenticator.
      #
      # @since 2.0.0
      def gssapi_authenticator
        hostname      = server.address.host
        servicename   = @opts[:gssapi_service_name] || 'mongodb'
        canonicalize  = @opts[:canonicalize_host_name] || false
        org.mongodb.sasl.GSSAPIAuthenticator.new(JRuby.runtime,
                                                 @username,
                                                 hostname,
                                                 servicename,
                                                 canonicalize)
      end

      # Return a startSasl message formatted for GSSAPI authentication.
      #
      # @return [ Mongo::Protocol::Query ] login message.
      #
      # @since 2.0.0
      def start_message
        payload = BSON::Binary.new(@authenticator.initialize_challenge)
        Mongo::Protocol::Query.new('$external',
                                   Mongo::Operation::COMMAND_COLLECTION_NAME,
                                   { :saslStart     => 1,
                                     :mechanism     => 'GSSAPI',
                                     :payload       => payload,
                                     :autoAuthorize => 1 })
      end

      # Return a saslContinue message formatted for GSSAPI authentication.
      #
      # @param [ Mongo::Protocol::Reply ] response Reply to previous message.
      #
      # @return [ Mongo::Protocol::Query ] saslContinue message.
      #
      # @since 2.0.0
      def continue_message(response)
        token = BSON::Binary.new(@authenticator.evaluate_challenge(response['payload'].to_s))
        Mongo::Protocol::Query.new('$external',
                                   Mongo::Operation::COMMAND_COLLECTION_NAME,
                                   { :saslContinue   => 1,
                                     :conversationId => response['conversationId'],
                                     :payload        => token })
      end
    end
  end
end

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
  module Sasl
    module GSSAPI

      def self.authenticate(username, client, socket, opts={})
        db           = client.db('$external')
        hostname     = socket.pool.host
        servicename  = opts[:gssapi_service_name] || 'mongodb'
        canonicalize = opts[:canonicalize_host_name] ? opts[:canonicalize_host_name] : false
        authenticator = Mongo::Sasl::GSSAPIAuthenticator.new(username, hostname, servicename, canonicalize)

        return { } unless authenticator.valid?

        token    = authenticator.initialize_challenge
        cmd      = BSON::OrderedHash['saslStart', 1, 'mechanism', 'GSSAPI', 'payload', token, 'autoAuthorize', 1]
        response = db.command(cmd, :check_response => false, :socket => socket)

        until response['done'] do
          break unless Support.ok?(response)
          token    = authenticator.evaluate_challenge(response['payload'])
          cmd      = BSON::OrderedHash['saslContinue', 1, 'conversationId', response['conversationId'], 'payload', token]
          response = db.command(cmd, :check_response => false, :socket => socket)
        end
        response
      end
    end
  end
end

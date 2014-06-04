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

# authenticatable mix-in
require 'mongo/auth/authenticatable'

# mechanism-specific auth types
require 'mongo/auth/x509'
require 'mongo/auth/mongo_cr'
require 'mongo/auth/plain'
require 'mongo/auth/gssapi'

module Mongo

  # Functionality for getting an object representing an authentication mechanism.
  #
  # @since 2.0.0
  module Auth
    extend self

    # Default mechanism for authentication.
    #
    # @since 2.0.0
    DEFAULT_MECHANISM = :'MONGODB-CR'

    # Hash lookup for the authenticatable classes, based off the symbols provided
    # in authenticating.
    #
    # @since 2.0.0
    MECHANISMS = {
      :'MONGODB-CR' => MongodbCR,
      :'MONGODB-X509' => X509,
      :PLAIN => Plain,
      :GSSAPI => GSSAPI
    }.freeze

    # Create an authentication object.
    #
    # @example Mongo::Auth.get(:PLAIN, opts)
    #
    # @param [ String ] db_name Name of the current database.
    # @param [ String ] username Username for authentication.
    # @param [ String, Symbol ] mechanism Mechanism to use for authentication.
    # @param [ Hash ] opts Options for this authenticator.
    #
    # @options opts [ String ] :password Password, required for some authentication
    #   mechanisms.
    # @options opts [ String ] :source Source, if different from current database.
    # @options opts [ String ] :gssapi_service_name For GSSAPI authentication
    #   only.
    # @options opts [ true, false ] :canonicalize_host_name For GSSAPI
    #   authentication only.
    #
    # @return [ Mongo::Auth ] Authentication object.
    #
    # @since 2.0.0
    def get(db_name, username, mechanism=DEFAULT_MECHANISM, opts={})
      mech = mechanism.upcase.to_sym
      unless MECHANISMS.has_key?(mech)
        raise ArgumentError,
        "Invalid authentication mechanism provided. Must be one of " +
          "#{MECHANISMS.keys.join(', ')}."
      end
      MECHANISMS.fetch(mech).new(db_name, username, opts)
    end
  end
end

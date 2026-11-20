# frozen_string_literal: true

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

require 'base64'
require 'forwardable'
require 'ipaddr'
require 'logger'
require 'openssl'
require 'rbconfig'
require 'resolv'
require 'securerandom'
require 'set'
require 'socket'
require 'stringio'
require 'timeout'
require 'uri'
require 'zlib'

autoload :CGI, 'cgi'

require 'bson'

require 'mongo/id'
require 'mongo/bson'
require 'mongo/semaphore'
require 'mongo/distinguishing_semaphore'
require 'mongo/condition_variable'
require 'mongo/csot_timeout_holder'
require 'mongo/options'
require 'mongo/loggable'
require 'mongo/deprecations'
require 'mongo/cluster_time'
require 'mongo/topology_version'
require 'mongo/monitoring'
require 'mongo/logger'
require 'mongo/retryable'
require 'mongo/operation'
require 'mongo/error'
require 'mongo/event'
require 'mongo/address'
require 'mongo/auth'
require 'mongo/protocol'
require 'mongo/background_thread'
require 'mongo/cluster'
require 'mongo/cursor'
require 'mongo/caching_cursor'
require 'mongo/collection'
require 'mongo/database'
require 'mongo/crypt'
require 'mongo/client' # Purposely out-of-order so that database is loaded first
require 'mongo/client_encryption'
require 'mongo/dbref'
require 'mongo/grid'
require 'mongo/index'
require 'mongo/search_index/view'
require 'mongo/lint'
require 'mongo/query_cache'
require 'mongo/server'
require 'mongo/server_selector'
require 'mongo/session'
require 'mongo/socket'
require 'mongo/srv'
require 'mongo/timeout'
require 'mongo/tracing'
require 'mongo/uri'
require 'mongo/version'
require 'mongo/write_concern'
require 'mongo/utils'
require 'mongo/config'

module Mongo
  class << self
    extend Forwardable

    # Delegate the given option along with its = and ? methods to the given
    # object.
    #
    # @param [ Object ] obj The object to delegate to.
    # @param [ Symbol ] opt The method to delegate.
    def self.delegate_option(obj, opt)
      def_delegators obj, opt, "#{opt}=", "#{opt}?"
    end

    # Take all the public instance methods from the Config singleton and allow
    # them to be accessed through the Mongo module directly.
    def_delegators Config, :options=
    delegate_option Config, :broken_view_aggregate
    delegate_option Config, :broken_view_options
    delegate_option Config, :validate_update_replace
    delegate_option Config, :csfle_convert_to_ruby_types
    delegate_option Config, :include_server_address_in_errors

    # @!scope class

    # @!method options=(options)
    #   Set the global driver configuration options.
    #   @param [ Hash ] options The options, keyed by option name.
    #   @api public

    # @!method broken_view_aggregate
    #   @return [ true | false ] Whether views keep the legacy aggregate behavior.
    #   @api public
    # @!method broken_view_aggregate=(value)
    #   @param [ true | false ] value The new value.
    #   @api public
    # @!method broken_view_aggregate?
    #   @return [ true | false ] Same as broken_view_aggregate.
    #   @api public

    # @!method broken_view_options
    #   @return [ true | false ] Whether views keep the legacy option handling.
    #   @api public
    # @!method broken_view_options=(value)
    #   @param [ true | false ] value The new value.
    #   @api public
    # @!method broken_view_options?
    #   @return [ true | false ] Same as broken_view_options.
    #   @api public

    # @!method validate_update_replace
    #   @return [ true | false ] Whether update and replace documents are validated.
    #   @api public
    # @!method validate_update_replace=(value)
    #   @param [ true | false ] value The new value.
    #   @api public
    # @!method validate_update_replace?
    #   @return [ true | false ] Same as validate_update_replace.
    #   @api public

    # @!method csfle_convert_to_ruby_types
    #   @return [ true | false ] Whether explicit decryption converts to Ruby types.
    #   @api public
    # @!method csfle_convert_to_ruby_types=(value)
    #   @param [ true | false ] value The new value.
    #   @api public
    # @!method csfle_convert_to_ruby_types?
    #   @return [ true | false ] Same as csfle_convert_to_ruby_types.
    #   @api public

    # @!method include_server_address_in_errors
    #   @return [ true | false ] Whether error messages include the server address.
    #   @api public
    # @!method include_server_address_in_errors=(value)
    #   @param [ true | false ] value The new value.
    #   @api public
    # @!method include_server_address_in_errors?
    #   @return [ true | false ] Same as include_server_address_in_errors.
    #   @api public
  end

  # Clears the driver's OCSP response cache.
  #
  # @api public
  module_function def clear_ocsp_cache
    Socket::OcspCache.clear
  end

  # This is a user-settable list of hooks that will be invoked when any new
  # TLS socket is connected. Each hook should be a Proc that takes
  # an OpenSSL::SSL::SSLContext object as an argument. These hooks can be used
  # to modify the TLS context (for example to disallow certain ciphers).
  #
  # @return [ Array<Proc> ] The list of procs to be invoked when a TLS socket
  #   is connected (may be an empty Array).
  #
  # @api public
  module_function def tls_context_hooks
    @tls_context_hooks ||= []
  end

  # Set the TLS context hooks.
  #
  # @param [ Array<Proc> ] hooks An Array of Procs, each of which should take
  #   an OpenSSL::SSL::SSLContext object as an argument.
  #
  # @api public
  module_function def tls_context_hooks=(hooks)
    unless hooks.is_a?(Array) && hooks.all? { |hook| hook.is_a?(Proc) }
      raise ArgumentError, 'TLS context hooks must be an array of Procs'
    end

    @tls_context_hooks = hooks
  end
end

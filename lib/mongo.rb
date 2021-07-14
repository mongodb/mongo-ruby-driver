# frozen_string_literal: true
# encoding: utf-8

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
require 'mongo/generation_generator'
require 'mongo/bson'
require 'mongo/semaphore'
require 'mongo/distinguishing_semaphore'
require 'mongo/options'
require 'mongo/loggable'
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
require 'mongo/lint'
require 'mongo/query_cache'
require 'mongo/server'
require 'mongo/server_selector'
require 'mongo/session'
require 'mongo/socket'
require 'mongo/srv'
require 'mongo/timeout'
require 'mongo/uri'
require 'mongo/version'
require 'mongo/write_concern'
require 'mongo/utils'

module Mongo
  # Clears the driver's OCSP response cache.
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
  module_function def tls_context_hooks
    @tls_context_hooks ||= []
  end

  # Set the TLS context hooks.
  #
  # @param [ Array<Proc> ] hooks An Array of Procs, each of which should take
  #   an OpenSSL::SSL::SSLContext object as an argument.
  module_function def tls_context_hooks=(hooks)
    unless hooks.is_a?(Array) && hooks.all? { |hook| hook.is_a?(Proc) }
      raise ArgumentError, "TLS context hooks must be an array of Procs"
    end

    @tls_context_hooks = hooks
  end
end

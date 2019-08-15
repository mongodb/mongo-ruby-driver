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

require 'bson'

require 'mongo/id'
require 'mongo/bson'
require 'mongo/reentrant_mutex'
require 'mongo/semaphore'
require 'mongo/options'
require 'mongo/loggable'
require 'mongo/cluster_time'
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
require 'mongo/collection'
require 'mongo/database'
require 'mongo/crypt'
require 'mongo/client' # Purposely out-of-order so that database is loaded first
require 'mongo/dbref'
require 'mongo/grid'
require 'mongo/index'
require 'mongo/server'
require 'mongo/server_selector'
require 'mongo/session'
require 'mongo/socket'
require 'mongo/srv'
require 'mongo/timeout'
require 'mongo/uri'
require 'mongo/version'
require 'mongo/write_concern'
require 'mongo/lint'
require 'mongo/client_encryption'

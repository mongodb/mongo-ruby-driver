# encoding: UTF-8
#
# --
# Copyright (C) 2008-2011 10gen Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
# ++

require 'mongo/version'

module Mongo
  ASCENDING   =  1
  DESCENDING  = -1
  GEO2D       = '2d'
  GEOHAYSTACK = 'geoHaystack'

  DEFAULT_MAX_BSON_SIZE = 4 * 1024 * 1024

  module Constants
    OP_REPLY        = 1
    OP_MSG          = 1000
    OP_UPDATE       = 2001
    OP_INSERT       = 2002
    OP_QUERY        = 2004
    OP_GET_MORE     = 2005
    OP_DELETE       = 2006
    OP_KILL_CURSORS = 2007

    OP_QUERY_TAILABLE          = 2 ** 1
    OP_QUERY_SLAVE_OK          = 2 ** 2
    OP_QUERY_OPLOG_REPLAY      = 2 ** 3
    OP_QUERY_NO_CURSOR_TIMEOUT = 2 ** 4
    OP_QUERY_AWAIT_DATA        = 2 ** 5
    OP_QUERY_EXHAUST           = 2 ** 6

    REPLY_CURSOR_NOT_FOUND     = 2 ** 0
    REPLY_QUERY_FAILURE        = 2 ** 1
    REPLY_SHARD_CONFIG_STALE   = 2 ** 2
    REPLY_AWAIT_CAPABLE        = 2 ** 3
  end
end

require 'bson'

require 'mongo/util/conversions'
require 'mongo/util/support'
require 'mongo/util/core_ext'
require 'mongo/util/logging'
require 'mongo/util/node'
require 'mongo/util/pool'
require 'mongo/util/pool_manager'
require 'mongo/util/server_version'
require 'mongo/util/ssl_socket'
require 'mongo/util/uri_parser'

require 'mongo/collection'
require 'mongo/networking'
require 'mongo/connection'
require 'mongo/repl_set_connection'
require 'mongo/cursor'
require 'mongo/db'
require 'mongo/exceptions'
require 'mongo/gridfs/grid_ext'
require 'mongo/gridfs/grid'
require 'mongo/gridfs/grid_io'
if RUBY_PLATFORM =~ /java/
  require 'mongo/gridfs/grid_io_fix'
end
require 'mongo/gridfs/grid_file_system'

require 'timeout'
Mongo::TimeoutHandler = Timeout

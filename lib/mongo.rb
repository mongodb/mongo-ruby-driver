# Copyright (C) 2009-2013 MongoDB, Inc.
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
  ASCENDING   =  1
  DESCENDING  = -1
  GEO2D       = '2d'
  GEO2DSPHERE = '2dsphere'
  GEOHAYSTACK = 'geoHaystack'
  TEXT        = 'text'
  HASHED      = 'hashed'

  INDEX_TYPES = {
    'ASCENDING'   => ASCENDING,
    'DESCENDING'  => DESCENDING,
    'GEO2D'       => GEO2D,
    'GEO2DSPHERE' => GEO2DSPHERE,
    'GEOHAYSTACK' => GEOHAYSTACK,
    'TEXT'        => TEXT,
    'HASHED'      => HASHED
  }

  DEFAULT_MAX_BSON_SIZE = 4 * 1024 * 1024
  MESSAGE_SIZE_FACTOR = 2

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
    OP_QUERY_PARTIAL           = 2 ** 7

    REPLY_CURSOR_NOT_FOUND     = 2 ** 0
    REPLY_QUERY_FAILURE        = 2 ** 1
    REPLY_SHARD_CONFIG_STALE   = 2 ** 2
    REPLY_AWAIT_CAPABLE        = 2 ** 3
  end

  module ErrorCode # MongoDB Core Server src/mongo/base/error_codes.err
    BAD_VALUE                = 2
    UNKNOWN_ERROR            = 8
    INVALID_BSON             = 22
    WRITE_CONCERN_FAILED     = 64
    MULTIPLE_ERRORS_OCCURRED = 65
    UNAUTHORIZED             = 13

    # mongod/s 2.6 and above return code 59 when a command doesn't exist.
    # mongod versions previous to 2.6 and mongos 2.4.x return no error code
    # when a command does exist.
    # mongos versions previous to 2.4.0 return code 13390 when a command
    # does not exist.
    COMMAND_NOT_FOUND_CODES  = [nil, 59, 13390]
  end
end

require 'bson'

require 'set'
require 'thread'

require 'mongo/utils'
require 'mongo/exception'
require 'mongo/functional'
require 'mongo/connection'
require 'mongo/collection_writer'
require 'mongo/collection'
require 'mongo/bulk_write_collection_view'
require 'mongo/cursor'
require 'mongo/db'
require 'mongo/gridfs'
require 'mongo/networking'
require 'mongo/mongo_client'
require 'mongo/mongo_replica_set_client'
require 'mongo/mongo_sharded_client'
require 'mongo/legacy'

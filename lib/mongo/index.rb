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

require 'mongo/index/view'

module Mongo
  # Contains constants for indexing purposes.
  #
  # @since 2.0.0
  module Index
    # Wildcard constant for all.
    #
    # @since 2.1.0
    ALL = '*'

    # Specify ascending order for an index.
    #
    # @since 2.0.0
    ASCENDING = 1

    # Specify descending order for an index.
    #
    # @since 2.0.0
    DESCENDING = -1

    # Specify a 2d Geo index.
    #
    # @since 2.0.0
    GEO2D = '2d'

    # Specify a 2d sphere Geo index.
    #
    # @since 2.0.0
    GEO2DSPHERE = '2dsphere'

    # Specify a geoHaystack index.
    #
    # @since 2.0.0
    # @deprecated
    GEOHAYSTACK = 'geoHaystack'

    # Encodes a text index.
    #
    # @since 2.0.0
    TEXT = 'text'

    # Specify a hashed index.
    #
    # @since 2.0.0
    HASHED = 'hashed'

    # Constant for the indexes collection.
    #
    # @since 2.0.0
    COLLECTION = 'system.indexes'
  end
end

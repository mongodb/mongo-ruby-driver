# frozen_string_literal: true
# rubocop:todo all

# Copyright (C) 2020 MongoDB Inc.
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
  # TopologyVersion encapsulates the topologyVersion document obtained from
  # hello responses and not master-like OperationFailure errors.
  #
  # @api private
  class TopologyVersion < BSON::Document
    def initialize(doc)
      if Lint.enabled?
        unless doc['processId']
          raise ArgumentError, 'Creating a topology version without processId field'
        end
        unless doc['counter']
          raise ArgumentError, 'Creating a topology version without counter field'
        end
      end

      super
    end

    # @return [ BSON::ObjectId ] The process id.
    def process_id
      self['processId']
    end

    # @return [ Integer ] The counter.
    def counter
      self['counter']
    end

    # Returns whether this topology version is potentially newer than another
    # topology version.
    #
    # Note that there is no total ordering of topology versions - given
    # two topology versions, each may be "potentially newer" than the other one.
    #
    # @param [ TopologyVersion ] other The other topology version.
    #
    # @return [ true | false ] Whether this topology version is potentially newer.
    # @api private
    def gt?(other)
      if process_id != other.process_id
        true
      else
        counter > other.counter
      end
    end

    # Returns whether this topology version is potentially newer than or equal
    # to another topology version.
    #
    # Note that there is no total ordering of topology versions - given
    # two topology versions, each may be "potentially newer" than the other one.
    #
    # @param [ TopologyVersion ] other The other topology version.
    #
    # @return [ true | false ] Whether this topology version is potentially newer.
    # @api private
    def gte?(other)
      if process_id != other.process_id
        true
      else
        counter >= other.counter
      end
    end

    # Converts the object to a document suitable for being sent to the server.
    #
    # @return [ BSON::Document ] The document.
    #
    # @api private
    def to_doc
      BSON::Document.new(self).merge(counter: BSON::Int64.new(counter))
    end
  end
end

# frozen_string_literal: true
# rubocop:todo all

# Copyright (C) 2019-2020 MongoDB Inc.
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
  # ClusterTime encapsulates cluster time storage and operations.
  #
  # The primary operation performed on the cluster time is advancing it:
  # given another cluster time, pick the newer of the two.
  #
  # This class provides comparison methods that are used to figure out which
  # cluster time is newer, and provides diagnostics in lint mode when
  # the actual time is missing from a cluster time document.
  #
  # @api private
  class ClusterTime < BSON::Document
    def initialize(elements = nil)
      super

      if Lint.enabled? && !self['clusterTime']
        raise ArgumentError, 'Creating a cluster time without clusterTime field'
      end
    end

    # Advances the cluster time in the receiver to the cluster time in +other+.
    #
    # +other+ can be nil or be behind the cluster time in the receiver; in
    # these cases the receiver is returned unmodified. If receiver is advanced,
    # a new ClusterTime object is returned.
    #
    # Return value is nil or a ClusterTime instance.
    def advance(other)
      if self['clusterTime'] && other['clusterTime'] &&
        other['clusterTime'] > self['clusterTime']
      then
        ClusterTime[other]
      else
        self
      end
    end

    # Compares two ClusterTime instances by comparing their timestamps.
    def <=>(other)
      if self['clusterTime'] && other['clusterTime']
        self['clusterTime'] <=> other['clusterTime']
      elsif !self['clusterTime']
        raise ArgumentError, "Cannot compare cluster times when receiver is missing clusterTime key: #{inspect}"
      else other['clusterTime']
        raise ArgumentError, "Cannot compare cluster times when other is missing clusterTime key: #{other.inspect}"
      end
    end

    # Older Rubies do not implement other logical operators through <=>.
    # TODO revise whether these methods are needed when
    # https://jira.mongodb.org/browse/RUBY-1622 is implemented.
    def >=(other)
      (self <=> other) != -1
    end
    def >(other)
      (self <=> other) == 1
    end
    def <=(other)
      (self <=> other) != 1
    end
    def <(other)
      (self <=> other) == -1
    end

    # Compares two ClusterTime instances by comparing their timestamps.
    def ==(other)
      if self['clusterTime'] && other['clusterTime'] &&
        self['clusterTime'] == other['clusterTime']
      then
        true
      else
        false
      end
    end

    class << self
      # Converts a BSON::Document to a ClusterTime.
      #
      # +doc+ can be nil, in which case nil is returned.
      def [](doc)
        if doc.nil? || doc.is_a?(ClusterTime)
          doc
        else
          ClusterTime.new(doc)
        end
      end
    end

    # This module provides common cluster time tracking behavior.
    #
    # @note Although attributes and methods defined in this module are part of
    #   the public API for the classes including this module, the fact that
    #   the methods are defined on this module and not directly on the
    #   including classes is not part of the public API.
    module Consumer

      # The cluster time tracked by the object including this module.
      #
      # @return [ nil | ClusterTime ] The cluster time.
      #
      # Changed in version 2.9.0: This attribute became an instance of
      # ClusterTime, which is a subclass of BSON::Document.
      # Previously it was an instance of BSON::Document.
      #
      # @since 2.5.0
      attr_reader :cluster_time

      # Advance the tracked cluster time document for the object including
      # this module.
      #
      # @param [ BSON::Document ] new_cluster_time The new cluster time document.
      #
      # @return [ ClusterTime ] The resulting cluster time.
      #
      # @since 2.5.0
      def advance_cluster_time(new_cluster_time)
        if @cluster_time
          @cluster_time = @cluster_time.advance(new_cluster_time)
        else
          @cluster_time = ClusterTime[new_cluster_time]
        end
      end
    end
  end
end

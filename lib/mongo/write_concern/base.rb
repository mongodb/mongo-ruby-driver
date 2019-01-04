# Copyright (C) 2014-2019 MongoDB, Inc.
#
# Licensed under the Apache License, Version 2.0 (the 'License');
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an 'AS IS' BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

module Mongo
  module WriteConcern

    # Defines common behavior for write concerns.
    #
    # @since 2.7.0
    class Base

      # @return [ Hash ] The write concern options.
      attr_reader :options

      # Instantiate a new write concern given the options.
      #
      # @api private
      #
      # @example Instantiate a new write concern mode.
      #   Mongo::WriteConcern::Acknowledged.new(:w => 1)
      #
      # @param [ Hash ] options The options to instantiate with.
      #
      # @option options :w [ Integer, String ] The number of servers or the
      #   custom mode to acknowledge.
      # @option options :j [ true, false ] Whether to acknowledge a write to
      #   the journal.
      # @option options :fsync [ true, false ] Should the write be synced to
      #   disc.
      # @option options :wtimeout [ Integer ] The number of milliseconds to
      #   wait for acknowledgement before raising an error.
      #
      # @since 2.0.0
      def initialize(options)
        opts =  Options::Mapper.transform_keys_to_symbols(options)
        @options = Options::Mapper.transform_values_to_strings(opts).freeze
      end

      # Is this write concern acknowledged.
      #
      # @example Whether this write concern object is acknowledged.
      #   write_concern.acknowledged?
      #
      # @return [ true, false ] Whether this write concern is acknowledged.
      #
      # @since 2.5.0
      def acknowledged?
        !!get_last_error
      end
    end
  end
end

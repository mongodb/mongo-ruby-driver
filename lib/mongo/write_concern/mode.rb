# Copyright (C) 2009-2013 MongoDB, Inc.
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

    # Defines default behavior for write concerns and provides a factory
    # interface to get a proper object from options.
    #
    # @since 2.0.0
    class Mode

      # The default write concern is to acknowledge on a single node.
      #
      # @since 2.0.0
      DEFAULT = { :w => 1 }.freeze

      # @return [ Hash ] The write concern options.
      attr_reader :options

      # Instantiate a new write concern given the options.
      #
      # @api private
      #
      # @example Instantiate a new write concern mode.
      #   Mongo::WriteConcern::Mode.new(:w => 1)
      #
      # @param [ Hash ] options The options to instantiate with.
      #
      # @option options :w [ Integer, String ] The number of nodes or the
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
        @options = options
      end

      private

      # Normalizes symbol option values into strings, since symbol will raise
      # an error on the server side with the gle.
      #
      # @api private
      #
      # @example Normalize the options.
      #   mode.normalize(:w => :majority)
      #
      # @param [ Hash ] options The options to normalize.
      #
      # @return [ Hash ] The hash with normalized values.
      #
      # @since 2.0.0
      def normalize(options)
        options.reduce({}) do |opts, (key, value)|
          opts[key] = value.is_a?(Symbol) ? value.to_s : value
          opts
        end
      end

      class << self

        # Get a write concern mode for the provided options.
        #
        # @example Get a write concern mode.
        #   Mongo::WriteConcern::Mode.get(:w => 1)
        #
        # @param [ Hash ] options The options to instantiate with.
        #
        # @option options :w [ Integer, String ] The number of nodes or the
        #   custom mode to acknowledge.
        # @option options :j [ true, false ] Whether to acknowledge a write to
        #   the journal.
        # @option options :fsync [ true, false ] Should the write be synced to
        #   disc.
        # @option options :wtimeout [ Integer ] The number of milliseconds to
        #   wait for acknowledgement before raising an error.
        #
        # @return [ Mongo::WriteConcern::Mode ] The appropriate node.
        #
        # @since 2.0.0
        def get(options)
          if unacknowledged?(options)
            Unacknowledged.new(options)
          else
            Acknowledged.new(options || DEFAULT)
          end
        end

        private

        # Determine if the options are for an unacknowledged write concern.
        #
        # @api private
        #
        # @param [ Hash ] options The options to check.
        #
        # @return [ true, false ] If the options are unacknowledged.
        #
        # @since 2.0.0
        def unacknowledged?(options)
          options && (options[:w] == 0 || options[:w] == -1)
        end
      end
    end
  end
end

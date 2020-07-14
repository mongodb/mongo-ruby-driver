# Copyright (C) 2020 MongoDB Inc.
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

  # @api private
  module Utils

    class LocalLogger
      include Loggable

      def initialize(**opts)
        @options = opts
      end

      attr_reader :options
    end

    # @option opts [ true | false | nil | Integer ] :bg_error_backtrace
    #   Experimental. Set to true to log complete backtraces for errors in
    #   background threads. Set to false or nil to not log backtraces. Provide
    #   a positive integer to log up to that many backtrace lines.
    # @option opts [ Logger ] :logger A custom logger to use.
    # @option opts [ String ] :log_prefix A custom log prefix to use when
    #   logging.
    module_function def warn_monitor_exception(msg, exc, **opts)
      bt_excerpt = excerpt_backtrace(exc, **opts)
      logger = LocalLogger.new(**opts)
      logger.log_warn("#{msg}: #{exc.class}: #{exc}#{bt_excerpt}")
    end

    # @option opts [ true | false | nil | Integer ] :bg_error_backtrace
    #   Experimental. Set to true to log complete backtraces for errors in
    #   background threads. Set to false or nil to not log backtraces. Provide
    #   a positive integer to log up to that many backtrace lines.
    module_function def excerpt_backtrace(exc, **opts)
      case lines = opts[:bg_error_backtrace]
      when Integer
        ":\n#{exc.backtrace[0..lines].join("\n")}"
      when false, nil
        nil
      else
        ":\n#{exc.backtrace.join("\n")}"
      end
    end

    module_function def shallow_symbolize_keys(hash)
      Hash[hash.map { |k, v| [k.to_sym, v] }]
    end
  end
end

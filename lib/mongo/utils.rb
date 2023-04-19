# frozen_string_literal: true
# rubocop:todo all

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
    module_function def warn_bg_exception(msg, exc, **opts)
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

    # Symbolizes the keys in the provided hash.
    module_function def shallow_symbolize_keys(hash)
      Hash[hash.map { |k, v| [k.to_sym, v] }]
    end

    # Stringifies the keys in the provided hash and converts underscore
    # style keys to camel case style keys.
    module_function def shallow_camelize_keys(hash)
      Hash[hash.map { |k, v| [camelize(k), v] }]
    end

    module_function def camelize(sym)
      sym.to_s.gsub(/_(\w)/) { $1.upcase }
    end

    # @note server_api must have symbol keys or be a BSON::Document.
    module_function def transform_server_api(server_api)
      {}.tap do |doc|
        if version = server_api[:version]
          doc['apiVersion'] = version
        end
        unless server_api[:strict].nil?
          doc['apiStrict'] = server_api[:strict]
        end
        unless server_api[:deprecation_errors].nil?
          doc['apiDeprecationErrors'] = server_api[:deprecation_errors]
        end
      end
    end

    # This function should be used if you need to measure time.
    # @example Calculate elapsed time.
    #   starting = Utils.monotonic_time
    #   # do something time consuming
    #   ending = Utils.monotonic_time
    #   puts "It took #{(ending - starting).to_i} seconds"
    #
    # @see https://blog.dnsimple.com/2018/03/elapsed-time-with-ruby-the-right-way/
    #
    # @return [Float] seconds according to monotonic clock
    module_function def monotonic_time
      Process.clock_gettime(Process::CLOCK_MONOTONIC)
    end
  end
end

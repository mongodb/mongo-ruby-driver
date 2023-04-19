# frozen_string_literal: true
# rubocop:todo all

module Mongo
  class Error
    # @note Although not_master? and node_recovering? methods of this module
    #   are part of the public API, the fact that these methods are defined on
    #   this module and not on the classes which include this module is not
    #   part of the public API.
    #
    # @api semipublic
    module SdamErrorDetection

      # @api private
      NOT_MASTER_CODES = [10107, 13435].freeze

      # @api private
      NODE_RECOVERING_CODES = [11600, 11602, 13436, 189, 91, 10058].freeze

      # @api private
      NODE_SHUTTING_DOWN_CODES = [11600, 91].freeze

      # Whether the error is a "not master" error, or one of its variants.
      #
      # See https://github.com/mongodb/specifications/blob/master/source/server-discovery-and-monitoring/server-discovery-and-monitoring.rst#not-master-and-node-is-recovering.
      #
      # @return [ true | false ] Whether the error is a not master.
      #
      # @since 2.8.0
      def not_master?
        # Require the error to be communicated at the top level of the response
        # for it to influence SDAM state. See DRIVERS-1376 / RUBY-2516.
        return false if document && document['ok'] == 1

        if node_recovering?
          false
        elsif code
          NOT_MASTER_CODES.include?(code)
        elsif message
          message.include?('not master')
        else
          false
        end
      end

      # Whether the error is a "node is recovering" error, or one of its variants.
      #
      # See https://github.com/mongodb/specifications/blob/master/source/server-discovery-and-monitoring/server-discovery-and-monitoring.rst#not-master-and-node-is-recovering.
      #
      # @return [ true | false ] Whether the error is a node is recovering.
      #
      # @since 2.8.0
      def node_recovering?
        # Require the error to be communicated at the top level of the response
        # for it to influence SDAM state. See DRIVERS-1376 / RUBY-2516.
        return false if document && document['ok'] == 1

        if code
          NODE_RECOVERING_CODES.include?(code)
        elsif message
          message.include?('node is recovering') || message.include?('not master or secondary')
        else
          false
        end
      end

      # Whether the error is a "node is shutting down" type error.
      #
      # See https://github.com/mongodb/specifications/blob/master/source/server-discovery-and-monitoring/server-discovery-and-monitoring.rst#not-master-and-node-is-recovering.
      #
      # @return [ true | false ] Whether the error is a node is shutting down.
      #
      # @since 2.9.0
      def node_shutting_down?
        if code && NODE_SHUTTING_DOWN_CODES.include?(code)
          true
        else
          false
        end
      end
    end
  end
end

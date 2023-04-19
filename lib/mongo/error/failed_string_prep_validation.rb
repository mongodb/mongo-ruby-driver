# frozen_string_literal: true
# rubocop:todo all

module Mongo
  class Error
    # This exception is raised when stringprep validation fails, such as due to
    #   character being present or bidirection data being invalid.
    #
    # @since 2.6.0
    class FailedStringPrepValidation < Error
      # The error message describing failed bidi validation.
      #
      # @since 2.6.0
      INVALID_BIDIRECTIONAL = 'Data failed bidirectional validation'.freeze

      # The error message describing the discovery of a prohibited character.
      #
      # @since 2.6.0
      PROHIBITED_CHARACTER = 'Data contains a prohibited character.'.freeze

      # The error message describing that stringprep normalization can't be done on Ruby versions
      # below 2.2.0.
      #
      # @since 2.6.0
      UNABLE_TO_NORMALIZE = 'Unable to perform normalization with Ruby versions below 2.2.0'.freeze

      # Create the new exception.
      #
      # @example Create the new exception.
      #   Mongo::Error::FailedStringPrepValidation.new(
      #     Mongo::Error::FailedStringPrepValidation::PROHIBITED_CHARACTER)
      #
      # @param [ String ] msg The error message describing how the validation failed.
      #
      # @since 2.6.0
      def initialize(msg)
        super(msg)
      end
    end
  end
end

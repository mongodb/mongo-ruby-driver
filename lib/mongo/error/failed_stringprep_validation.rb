module Mongo
  class Error
    # This exception is raised when stringprep validation fails, such as due to a prohibited
    #   character being present or bidirection data being invalid.
    #
    # @since 2.6.0
    class FailedStringPrepValidation < Error
      # The error message describing failed bidi validation.
      #
      # @since 2.6.0
      INVALID_BIDIRECTIONAL = 'StringPrep bidirectional data is invalid'.freeze

      # The error message describing the discovery of a prohibited character.
      #
      # @since 2.6.0
      PROHIBITED_CHARACTER = 'StringPrep data contains a prohibited character.'.freeze

      # Create the new exception.
      #
      # @example Create the new exception.
      #   Mongo::Error::FailedStringPrepValidation.new(
      #     Mongo::Error::FailedStringPrepValidation::PROHIBITED_CHARACTER)
      #
      # @param [ String ] msg The error message describing how the validation failed.
      def initialize(msg)
        super(msg)
      end
    end
  end
end

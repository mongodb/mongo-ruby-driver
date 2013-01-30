module BSON

  # JavaScript code to be evaluated by MongoDB.
  class Code

    # Hash mapping identifiers to their values
    attr_accessor :scope, :code

    # Wrap code to be evaluated by MongoDB.
    #
    # @param [String] code the JavaScript code.
    # @param [Hash] a document mapping identifiers to values, which
    #   represent the scope in which the code is to be executed.
    def initialize(code, scope={})
      @code  = code
      @scope = scope

      unless @code.is_a?(String)
        raise ArgumentError, "BSON::Code must be in the form of a String; #{@code.class} is not allowed."
      end
    end

    def length
      @code.length
    end

    def ==(other)
      self.class == other.class &&
        @code == other.code && @scope == other.scope
    end

    def inspect
      "<BSON::Code:#{object_id} @data=\"#{@code}\" @scope=\"#{@scope.inspect}\">"
    end

    def to_bson_code
      self
    end

  end
end

# Copyright (C) 2009-2013 MongoDB, Inc.
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

module BSON

  # generates a wrapped Regexp with lazy compilation.
  # can represent flags not supported in Ruby's core Regexp class before compilation.
  class Regex

    IGNORECASE       = 1
    EXTENDED         = IGNORECASE<<1
    MULTILINE        = EXTENDED<<1
    DOTALL           = MULTILINE<<1
    LOCALE_DEPENDENT = DOTALL<<1
    UNICODE          = LOCALE_DEPENDENT<<1

    attr_accessor :pattern
    alias_method  :source, :pattern
    attr_accessor :options

    # Create a new regexp.
    #
    # @param pattern [String]
    # @param options [Array, String]
    def initialize(pattern, *opts)
      @pattern = pattern
      @options = opts.first.is_a?(Fixnum) ? opts.first : str_opts_to_int(opts.join)
    end

    # Attempt to convert a native Ruby Regexp to a BSON::Regex.
    #
    # @param regexp [Regexp] The native Ruby regexp object to convert to BSON::Regex.
    #
    # @return [BSON::Regex]
    def self.from_native(regexp)
      warn 'Ruby Regexps use different syntax and set of flags than BSON regular expressions.'
      pattern = regexp.source
      opts = 0
      opts |= IGNORECASE if (Regexp::IGNORECASE & regexp.options != 0)
      opts |= DOTALL     if (Regexp::MULTILINE  & regexp.options != 0)
      opts |= EXTENDED   if (Regexp::EXTENDED   & regexp.options != 0)
      self.new(pattern, opts)
    end

    # Check equality of this wrapped Regexp with another.
    #
    # @param [BSON::Regex] regexp
    def eql?(regexp)
      regexp.kind_of?(BSON::Regex) &&
        self.pattern == regexp.pattern &&
        self.options == regexp.options
    end
    alias_method :==, :eql?

    # Get a human-readable representation of this BSON Regex.
    def inspect
      "#<BSON::Regex:0x#{self.object_id} " <<
      "@pattern=#{@pattern}>, @options=#{@options}>"
    end

    # Clone or dup the current BSON::Regex.
    def initialize_copy
      a_copy = self.dup
      a_copy.pattern = self.pattern.dup
      a_copy.options = self.options.dup
      a_copy
    end

    # Compile the BSON::Regex.
    #
    # @return [Regexp] A ruby core Regexp object.
    def try_compile
      warn 'Regular expressions retreived from the server may contain a pattern or flags ' <<
           'not supported by Ruby Regexp objects.'
      regexp_opts = 0
      regexp_opts |= Regexp::IGNORECASE if (options & IGNORECASE != 0)
      regexp_opts |= Regexp::MULTILINE  if (options & DOTALL != 0)
      regexp_opts |= Regexp::EXTENDED   if (options & EXTENDED != 0)
      Regexp.new(pattern, regexp_opts)
    end

    private
    # Convert the string options to an integer.
    #
    # @return [Fixnum] The Integer representation of the options.
    def str_opts_to_int(str_opts="")
      opts = 0
      opts |= IGNORECASE       if str_opts.include?('i')
      opts |= LOCALE_DEPENDENT if str_opts.include?('l')
      opts |= MULTILINE        if str_opts.include?('m')
      opts |= DOTALL           if str_opts.include?('s')
      opts |= UNICODE          if str_opts.include?('u')
      opts |= EXTENDED         if str_opts.include?('x')
      opts
    end
  end
end

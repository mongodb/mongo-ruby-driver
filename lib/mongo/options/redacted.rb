# Copyright (C) 2015 MongoDB, Inc.
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
  module Options

    REDACTED_OPTIONS = [:password, :pwd]
    REDACTED_STRING = '<REDACTED>'

    class Redacted < BSON::Document

      def inspect
        '{' + reduce([]) do |list, (k, v)|
          list << "#{k.inspect}=>#{redact(k, v, __method__)}"
        end.join(', ') + '}'
      end

      def to_s
        '{' + reduce([]) do |list, (k, v)|
          list << "#{k.to_s}=>#{redact(k, v, __method__)}"
        end.join(', ') + '}'
      end

      def has_key?(key)
        super(convert_key(key))
      end

      private

      def redact(k, v, method)
        return REDACTED_STRING if REDACTED_OPTIONS.include?(k.to_sym)
        v.send(method)
      end
    end
  end
end

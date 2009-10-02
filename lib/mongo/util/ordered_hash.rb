# --
# Copyright (C) 2008-2009 10gen Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
# ++

# A hash in which the order of keys are preserved.
#
# Under Ruby 1.9 and greater, this class has no added methods because Ruby's
# Hash already keeps its keys ordered by order of insertion.
class OrderedHash < Hash

  def ==(other)
    begin
      !other.nil? &&
        keys == other.keys &&
        values == other.values
    rescue
      false
    end
  end

  # We only need the body of this class if the RUBY_VERSION is before 1.9
  if RUBY_VERSION < '1.9'
    attr_accessor :ordered_keys

    def self.[] *args
      oh = OrderedHash.new
      if Hash === args[0]
        oh.merge! args[0]
      elsif (args.size % 2) != 0
        raise ArgumentError, "odd number of elements for Hash"
      else
        0.step(args.size - 1, 2) do |key|
          value = key + 1
          oh[args[key]] = args[value]
        end
      end
      oh
    end

    def initialize(*a, &b)
      super
      @ordered_keys = []
    end

    def keys
      @ordered_keys || []
    end

    def []=(key, value)
      @ordered_keys ||= []
      @ordered_keys << key unless @ordered_keys.include?(key)
      super(key, value)
    end

    def each
      @ordered_keys ||= []
      @ordered_keys.each { |k| yield k, self[k] }
      self
    end

    def values
      collect { |k, v| v }
    end

    def merge(other)
      oh = self.dup
      oh.merge!(other)
      oh
    end

    def merge!(other)
      @ordered_keys ||= []
      @ordered_keys += other.keys # unordered if not an OrderedHash
      @ordered_keys.uniq!
      super(other)
    end

    def inspect
      str = '{'
      str << (@ordered_keys || []).collect { |k| "\"#{k}\"=>#{self.[](k).inspect}" }.join(", ")
      str << '}'
    end

    def delete(key, &block)
      @ordered_keys.delete(key) if @ordered_keys
      super
    end

    def delete_if(&block)
      self.each { |k,v|
        if yield k, v
          delete(k)
        end
      }
    end

    def clear
      super
      @ordered_keys = []
    end
  end
end

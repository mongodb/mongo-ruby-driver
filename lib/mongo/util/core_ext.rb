# encoding: UTF-8

# --
# Copyright (C) 2008-2011 10gen Inc.
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

#:nodoc:
class Object

  #:nodoc:
  def tap
    yield self
    self
  end unless respond_to? :tap

end

#:nodoc:
module Enumerable

  #:nodoc:
  def each_with_object(memo)
    each { |element| yield(element, memo) }
    memo
  end unless [].respond_to?(:each_with_object)

end

#:nodoc:
class Hash

  #:nodoc:
  def assert_valid_keys(*valid_keys)
    unknown_keys = keys - [valid_keys].flatten
    raise(ArgumentError, "Unknown key(s): #{unknown_keys.join(", ")}") unless unknown_keys.empty?
  end

end

#:nodoc:
class String

  #:nodoc:
  def to_bson_code
    BSON::Code.new(self)
  end

end

#:nodoc:
class Class
  def mongo_thread_local_accessor name, options = {}
    m = Module.new
    m.module_eval do
      class_variable_set :"@@#{name}", Hash.new {|h,k| h[k] = options[:default] }
    end
    m.module_eval %{

      def #{name}
        @@#{name}[Thread.current.object_id]
      end

      def #{name}=(val)
        @@#{name}[Thread.current.object_id] = val
      end
    }

    class_eval do
      include m
      extend m
    end
  end
end

# Fix a bug in the interaction of
# mutexes and timeouts in Ruby 1.9.
# See https://jira.mongodb.org/browse/RUBY-364 for details.
if RUBY_VERSION > '1.9'
  class Mutex
    def lock_with_hack
      lock_without_hack
      rescue ThreadError => e
      if e.message != "deadlock; recursive locking"
        raise
      else
        unlock
        lock_without_hack
      end
    end
      alias_method :lock_without_hack, :lock
      alias_method :lock, :lock_with_hack
  end
end

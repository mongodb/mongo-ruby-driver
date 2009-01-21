# --
# Copyright (C) 2008-2009 10gen Inc.
#
# This program is free software: you can redistribute it and/or modify it
# under the terms of the GNU Affero General Public License, version 3, as
# published by the Free Software Foundation.
#
# This program is distributed in the hope that it will be useful, but WITHOUT
# ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
# FITNESS FOR A PARTICULAR PURPOSE. See the GNU Affero General Public License
# for more details.
#
# You should have received a copy of the GNU Affero General Public License
# along with this program. If not, see <http://www.gnu.org/licenses/>.
# ++

# A hash in which the order of keys are preserved.
#
# Under Ruby 1.9 and greater, this class has no added methods because Ruby's
# Hash already keeps its keys ordered by order of insertion.
class OrderedHash < Hash

  # We only need the body of this class if the RUBY_VERSION is before 1.9
  if RUBY_VERSION < '1.9'

  attr_accessor :ordered_keys

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
    @ordered_keys.delete(key)
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

  end                           # Ruby before 1.9

end

# frozen_string_literal: true

module Unified
  class EntityMap
    extend Forwardable

    def initialize
      @map = {}
    end

    def set(type, id, value)
      @map[type] ||= {}
      if @map[type][id]
        raise Error::EntityMapOverwriteAttempt,
              "Cannot set #{type} #{id} because it is already defined"
      end
      @map[type][id] = value
    end

    def get(type, id)
      raise Error::EntityMissing, "There are no #{type} entities known" unless @map[type]
      unless v = @map[type][id]
        raise Error::EntityMissing, "There is no #{type} #{id} known"
      end

      v
    end

    def get_any(id)
      @map.each do |_type, sub|
        return sub[id] if sub[id]
      end
      raise Error::EntityMissing, "There is no #{id} known"
    end

    def_delegators :@map, :[], :fetch
  end
end

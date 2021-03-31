# frozen_string_literal: true
# encoding: utf-8

module Unified
  class EntityMap
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
      unless @map[type]
        raise Error::EntityMissing, "There are no #{type} entities known"
      end
      unless v = @map[type][id]
        raise Error::EntityMissing, "There is no #{type} #{id} known"
      end
      v
    end

    def get_any(id)
      @map.each do |type, sub|
        if sub[id]
          return sub[id]
        end
      end
      raise Error::EntityMissing, "There is no #{id} known"
    end

    def [](type)
      @map[type]
    end
  end
end

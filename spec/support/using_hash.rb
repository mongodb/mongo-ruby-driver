# frozen_string_literal: true
# encoding: utf-8

class UsingHash < Hash
  def use(key)
    wrap(self[key]).tap do
      delete(key)
    end
  end

  def use!(key)
    wrap(fetch(key)).tap do
      delete(key)
    end
  end

  private

  def wrap(v)
    case v
    when Hash
      self.class[v]
    when Array
      v.map do |subv|
        wrap(subv)
      end
    else
      v
    end
  end
end

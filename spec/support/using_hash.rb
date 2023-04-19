# frozen_string_literal: true
# rubocop:todo all

class UsingHash < Hash
  class UsingHashKeyError < KeyError
  end

  def use(key)
    wrap(self[key]).tap do
      delete(key)
    end
  end

  def use!(key)
    begin
      value = fetch(key)
    rescue KeyError => e
      raise UsingHashKeyError, e.to_s
    end

    wrap(value).tap do
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

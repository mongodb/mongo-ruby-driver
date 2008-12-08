class OrderedHash < Hash

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

  def merge(other)
    @ordered_keys ||= []
    @ordered_keys += other.keys # unordered if not an OrderedHash
    super(other)
  end

end

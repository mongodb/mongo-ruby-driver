RSpec::Matchers.define :be_int32 do |num|
  match do |actual|
    actual == [num].pack('l<')
  end
end

RSpec::Matchers.define :be_int64 do |num|
  match do |actual|
    actual == [num].pack('q<')
  end
end

RSpec::Matchers.define :be_int64_sequence do |array|
  match do |actual|
    actual == array.reduce(String.new) do |buffer, num|
      buffer << [num].pack('q<')
    end
  end
end

RSpec::Matchers.define :be_cstring do |string|
  match do |actual|
    actual == "#{string.force_encoding(BSON::BINARY)}\0"
  end
end

RSpec::Matchers.define :be_bson do |hash|
  match do |actual|
    actual == hash.to_bson.to_s
  end
end

RSpec::Matchers.define :be_bson_sequence do |array|
  match do |actual|
    actual == array.map(&:to_bson).join
  end
end

RSpec::Matchers.define :be_ciphertext do
  match do |object|
    object.is_a?(BSON::Binary) && object.type == :ciphertext
  end
end

def match?(obj1, obj2)
  if obj1.is_a?(Hash) && obj1.key?('$$type')
    case obj1['$$type']
    when 'binData'
      obj2.is_a?(BSON::Binary)
    when 'long'
      obj2.key?('$numberLong')
    else
      raise "Must implement logic for #{v['$$type']}"
    end
  elsif obj1.is_a?(Hash) && obj2.is_a?(Hash)
    obj1.keys.all? do |key|
      match?(obj1[key], obj2[key])
    end
  elsif obj1.is_a?(Array) && obj2.is_a?(Array)
    obj1.map.with_index do |_, idx|
      match?(obj1[idx], obj2[idx])
    end.all?(true)
  else
    return obj1 == obj2
  end
end

RSpec::Matchers.define :match_event do |event|
  match do |actual|
    match?(event, actual)
  end
end

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


RSpec::Matchers.define :match_with_type do |event|
  match do |actual|
    Utils.match_with_type?(event, actual)
  end
end

RSpec::Matchers.define :be_uuid do
  match do |object|
    object.is_a?(BSON::Binary) && object.type == :uuid
  end
end

# frozen_string_literal: true
# rubocop:todo all

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
    actual == "#{string.dup.force_encoding(BSON::BINARY)}\0"
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

RSpec::Matchers.define :take_longer_than do |min_expected_time|
  match do |proc|
    start_time = Mongo::Utils.monotonic_time
    proc.call
    (Mongo::Utils.monotonic_time - start_time).should > min_expected_time
  end
end

RSpec::Matchers.define :take_shorter_than do |min_expected_time|
  match do |proc|
    start_time = Mongo::Utils.monotonic_time
    proc.call
    (Mongo::Utils.monotonic_time - start_time).should < min_expected_time
  end
end

RSpec::Matchers.define :be_explain_output do
  match do |actual|
    Hash === actual && (
      actual.key?('queryPlanner') ||
      actual.key?('allPlans')
    )
  end

  failure_message do |actual|
    "expected that #{actual} is explain output: is a hash with either allPlans or queryPlanner keys present"
  end
end

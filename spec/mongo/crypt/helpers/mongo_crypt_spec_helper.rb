# frozen_string_literal: true
# rubocop:todo all

module MongoCryptSpecHelper
  def bind_crypto_hooks(mongocrypt)
    Mongo::Crypt::Binding.mongocrypt_setopt_crypto_hooks(
      mongocrypt,
      method(:aes_encrypt),
      method(:aes_decrypt),
      method(:random),
      method(:hmac_sha_512),
      method(:hmac_sha_256),
      method(:hmac_hash),
      nil
    )
  end
  module_function :bind_crypto_hooks

  def mongocrypt_binary_t_from(string)
    bytes = string.unpack('C*')

    p = FFI::MemoryPointer
      .new(bytes.size)
      .write_array_of_type(FFI::TYPE_UINT8, :put_uint8, bytes)

    Mongo::Crypt::Binding.mongocrypt_binary_new_from_data(p, bytes.length)
  end
  module_function :mongocrypt_binary_t_from

  private

  def string_from_binary(binary_p)
    str_p = Mongo::Crypt::Binding.mongocrypt_binary_data(binary_p)
    len = Mongo::Crypt::Binding.mongocrypt_binary_len(binary_p)
    str_p.read_string(len)
  end
  module_function :string_from_binary

  def write_to_binary(binary_p, data)
    str_p = Mongo::Crypt::Binding.mongocrypt_binary_data(binary_p)
    str_p.put_bytes(0, data)
  end
  module_function :write_to_binary

  def aes_encrypt(_, key_binary_p, iv_binary_p, input_binary_p, output_binary_p,
    response_length_p, status_p)
    key = string_from_binary(key_binary_p)
    iv = string_from_binary(iv_binary_p)
    input = string_from_binary(input_binary_p)

    output = Mongo::Crypt::Hooks.aes(key, iv, input)
    write_to_binary(output_binary_p, output)
    response_length_p.write_int(output.length)

    true
  end
  module_function :aes_encrypt

  def aes_decrypt(_, key_binary_p, iv_binary_p, input_binary_p, output_binary_p,
    response_length_p, status_p)
    key = string_from_binary(key_binary_p)
    iv = string_from_binary(iv_binary_p)
    input = string_from_binary(input_binary_p)

    output = Mongo::Crypt::Hooks.aes(key, iv, input, decrypt: true)
    write_to_binary(output_binary_p, output)
    response_length_p.write_int(output.length)

    true
  end
  module_function :aes_decrypt

  def random(_, output_binary_p, num_bytes, status_p)
    output = Mongo::Crypt::Hooks.random(num_bytes)
    write_to_binary(output_binary_p, output)

    true
  end
  module_function :random

  def hmac_sha_512(_, key_binary_p, input_binary_p, output_binary_p, status_p)
    key = string_from_binary(key_binary_p)
    input = string_from_binary(input_binary_p)

    output = Mongo::Crypt::Hooks.hmac_sha('SHA512', key, input)
    write_to_binary(output_binary_p, output)

    true
  end
  module_function :hmac_sha_512

  def hmac_sha_256(_, key_binary_p, input_binary_p, output_binary_p, status_p)
    key = string_from_binary(key_binary_p)
    input = string_from_binary(input_binary_p)

    output = Mongo::Crypt::Hooks.hmac_sha('SHA256', key, input)
    write_to_binary(output_binary_p, output)

    true
  end
  module_function :hmac_sha_256

  def hmac_hash(_, input_binary_p, output_binary_p, status_p)
    input = string_from_binary(input_binary_p)
    output = Mongo::Crypt::Hooks.hash_sha256(input)
    write_to_binary(output_binary_p, output)

    true
  end
  module_function :hmac_hash
end

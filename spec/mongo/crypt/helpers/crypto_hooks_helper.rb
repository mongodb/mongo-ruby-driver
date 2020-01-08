module CryptoHooksHelper
  def bind_crypto_hooks(mongocrypt)
    Mongo::Crypt::Binding.mongocrypt_setopt_crypto_hooks(
      mongocrypt,
      Proc.new do |_, key_binary_p, iv_binary_p, input_binary_p, output_binary_p, response_length_p, status_p|
        Mongo::Crypt::Hooks.aes(key_binary_p, iv_binary_p, input_binary_p, output_binary_p, response_length_p, status_p)
      end,
      Proc.new do |_, key_binary_p, iv_binary_p, input_binary_p, output_binary_p, response_length_p, status_p|
        Mongo::Crypt::Hooks.aes(key_binary_p, iv_binary_p, input_binary_p, output_binary_p, response_length_p, status_p, decrypt: true)
      end,
      Proc.new do |_, output_binary_p, num_bytes, status_p|
        Mongo::Crypt::Hooks.random(output_binary_p, num_bytes, status_p)
      end,
      Proc.new do |_, key_binary_p, input_binary_p, output_binary_p, status_p|
        Mongo::Crypt::Hooks.hmac_sha('SHA512', key_binary_p, input_binary_p, output_binary_p, status_p)
      end,
      Proc.new do |_, key_binary_p, input_binary_p, output_binary_p, status_p|
        Mongo::Crypt::Hooks.hmac_sha('SHA256', key_binary_p, input_binary_p, output_binary_p, status_p)
      end,
      Proc.new do |_, input_binary_p, output_binary_p, status_p|
        Mongo::Crypt::Hooks.hash_sha256(input_binary_p, output_binary_p, status_p)
      end,
      nil
    )
  end
  module_function :bind_crypto_hooks
end

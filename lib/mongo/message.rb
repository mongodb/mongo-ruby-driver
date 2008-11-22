%w(get_more_message insert_message kill_cursors_message message_header
   msg_message query_message remove_message update_message).each { |f|
  require "mongo/message/#{f}"
}

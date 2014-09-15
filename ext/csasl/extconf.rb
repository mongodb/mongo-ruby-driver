require 'mkmf'
find_header('sasl/sasl.h')

if have_library('sasl2', 'sasl_version')
  create_makefile('csasl/csasl')
else
  abort "libsasl (cyrus sasl) is required in the system to install the mongo_kerberos gem."
end

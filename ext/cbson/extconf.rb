require 'mkmf'

have_header("ruby/st.h") || have_header("st.h")
have_header("ruby/regex.h") || have_header("regex.h")

dir_config('cbson')
create_makefile('mongo_ext/cbson')

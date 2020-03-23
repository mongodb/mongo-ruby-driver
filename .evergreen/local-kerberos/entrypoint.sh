#!/bin/sh

echo 127.0.0.1 krb.local >>/etc/hosts

krb5kdc
kadmind

# Check that the daemons are running:
#ps awwxu

# Check that kerberos is set up successfully and a user can authenticate:
#echo testp |kinit test/test@LOCALKRB

exec "$@"

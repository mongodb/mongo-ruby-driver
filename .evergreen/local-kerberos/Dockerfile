# https://help.ubuntu.com/lts/serverguide/kerberos.html

FROM ubuntu:bionic

ENV DEBIAN_FRONTEND=noninteractive

RUN apt-get update

RUN apt-get install -y krb5-kdc krb5-admin-server nvi less iproute2

COPY krb5.conf /etc/krb5.conf
COPY kdc.conf /etc/krb5kdc/kdc.conf
COPY kadm5.acl /etc/krb5kdc/kadm5.acl

RUN (echo masterp; echo masterp) |kdb5_util create -s

RUN (echo testp; echo testp) |kadmin.local addprinc test/test@LOCALKRB

COPY entrypoint.sh entrypoint.sh
ENTRYPOINT ["./entrypoint.sh"]
CMD ["tail", "-f", "/var/log/kdc.log"]

# Kerberos ports
EXPOSE 88
#EXPOSE 464
#EXPOSE 749

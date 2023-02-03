FROM ruby:3.1.3-bullseye

ENV DEBIAN_FRONTEND=noninteractive

RUN apt-get update && \
    apt-get -y install python3 python3-pip zsh sudo locales gnupg wget libsnmp-dev \
    # These packages are optional, feel free to adjust to your liking.
    fzf jq neovim tmux \
    && locale-gen en_US.UTF-8

ENV DEBIAN_FRONTEND=dialog

ENV USER_NAME dbx
ENV USER_PASSWORD password

RUN adduser --quiet --disabled-password --shell /bin/zsh --home /home/$USER_NAME --gecos "User" $USER_NAME
RUN echo "${USER_NAME}:${USER_PASSWORD}" | chpasswd && usermod -aG sudo $USER_NAME
RUN echo $USER_NAME ALL=\(root\) NOPASSWD:ALL > /etc/sudoers.d/$USER_NAME && chmod 0440 /etc/sudoers.d/$USER_NAME
ENV HOME /home/$USER_NAME
RUN echo "en_US.UTF-8 UTF-8" > /etc/locale.gen && /usr/sbin/locale-gen
USER $USER_NAME
RUN mkdir -p ${HOME}/.local/bin
ENV PATH ${HOME}/.local/bin:${PATH}
ENV TERM xterm
ENV SHELL /bin/zsh
# This is optional
RUN wget https://github.com/robbyrussell/oh-my-zsh/raw/master/tools/install.sh -O - | zsh || true
RUN git clone https://github.com/zsh-users/zsh-autosuggestions ${ZSH_CUSTOM:-~/.oh-my-zsh/custom}/plugins/zsh-autosuggestions

RUN pip3 install 'mtools[mlaunch]' -U --upgrade-strategy eager

ENV MONGODB_VERSION 6.2.0

RUN curl --retry 3 -fL -o ${HOME}/.local/bin/m https://raw.githubusercontent.com/aheckmann/m/master/bin/m && chmod +x ${HOME}/.local/bin/m
RUN yes | m ${MONGODB_VERSION}-ent
RUN mkdir -p ${HOME}/crypt_shared
WORKDIR ${HOME}/crypt_shared
RUN curl --retry 3 -fL "https://downloads.mongodb.com/linux/mongo_crypt_shared_v1-linux-x86_64-enterprise-debian11-${MONGODB_VERSION}.tgz" | tar zxf -
ENV MONGO_RUBY_DRIVER_CRYPT_SHARED_LIB_PATH=${HOME}/crypt_shared/lib/mongo_crypt_v1.so
RUN curl --retry 3 -fL -o ${HOME}/.local/bin/m https://raw.githubusercontent.com/aheckmann/m/master/bin/m && chmod +x ${HOME}/.local/bin/m

ENV MONGOSH_VERSION 1.6.2

RUN mkdir -p ${HOME}/mongosh
WORKDIR ${HOME}/mongosh
RUN curl --retry 3 -fL https://downloads.mongodb.com/compass/mongosh-${MONGOSH_VERSION}-linux-x64.tgz | tar zxf -
RUN ls -lah mongosh-${MONGOSH_VERSION}-linux-x64/bin
RUN sudo cp ./mongosh-${MONGOSH_VERSION}-linux-x64/bin/mongosh /usr/bin/ && sudo cp ./mongosh-${MONGOSH_VERSION}-linux-x64/bin/*.so /usr/lib/
ENV FLE=helper

WORKDIR /workspace

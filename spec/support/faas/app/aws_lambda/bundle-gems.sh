#!/bin/sh

# source:
# https://dev.to/aws-builders/building-aws-ruby-lambdas-that-require-gems-with-native-extension-17h

dir=`pwd`

bundle config set --local path 'vendor/bundle'
bundle config set --local deployment 'true'

docker run --platform=linux/amd64 \
  -e BUNDLE_SILENCE_ROOT_WARNING=1 \
  -v $dir:$dir \
  -w $dir \
  public.ecr.aws/sam/build-ruby3.2 \
  bundle install

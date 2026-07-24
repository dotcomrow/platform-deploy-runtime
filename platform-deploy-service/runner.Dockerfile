FROM node:22-alpine

ARG TERRAFORM_VERSION=1.13.5

RUN apk add --no-cache \
    bash \
    ca-certificates \
    curl \
    git \
    jq \
    openssl \
    python3 \
    unzip \
  && curl -fsSLo /tmp/terraform.zip "https://releases.hashicorp.com/terraform/${TERRAFORM_VERSION}/terraform_${TERRAFORM_VERSION}_linux_amd64.zip" \
  && unzip /tmp/terraform.zip -d /usr/local/bin \
  && rm /tmp/terraform.zip \
  && terraform version

WORKDIR /workspace
CMD ["/bin/sh"]

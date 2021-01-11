FROM public.ecr.aws/lambda/python:3.6.2021.01.06.16

RUN apt-get update

RUN apt-get install --no-install-recommends -y curl git dnsutils unzip python3-pip
RUN curl "https://awscli.amazonaws.com/awscli-exe-linux-x86_64.zip" -o "awscliv2.zip" \
    && unzip awscliv2.zip \
    && ./aws/install

RUN curl -sL https://deb.nodesource.com/setup_12.x | bash - && apt-get install -y nodejs

RUN chmod -R 777 $HOME/
RUN mkdir $HOME/.npm
USER $USER

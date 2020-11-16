FROM python:3.7-alpine AS build

ARG GITLAB_TOKEN
ARG SHA
RUN apk add --no-cache npm curl
RUN npm install -g sass
WORKDIR /src/francearchives
RUN curl --header "PRIVATE-TOKEN: $GITLAB_TOKEN" "https://forge.extranet.logilab.fr/api/v4/projects/380/repository/archive?sha=$SHA" | tar -xzf -
RUN mv cubicweb-francearchives-* cubicweb-francearchives
WORKDIR /src/francearchives/cubicweb-francearchives
RUN python ./setup.py sdist

WORKDIR /src/frarchives-edition

COPY package.json .
COPY package-lock.json .

COPY . .
RUN python ./setup.py sdist

FROM logilab/cubicweb:latest
USER root

COPY --from=build \
     /src/francearchives/cubicweb-francearchives/dist/cubicweb-francearchives-*.tar.gz \
     /src/cubicweb-francearchives.tar.gz
RUN pip install /src/cubicweb-francearchives.tar.gz

COPY --from=build \
     /src/frarchives-edition/dist/cubicweb-frarchives-edition-*.tar.gz \
     /src/cubicweb-frarchives-edition.tar.gz
RUN pip install /src/cubicweb-frarchives-edition.tar.gz
RUN pip install pyramid-debugtoolbar pyramid-redis-sessions

USER cubicweb
ENV CUBE=frarchives_edition
ENV CW_DB_NAME=${CUBE}
ENV CW_ANONYMOUS_USER=anon
ENV CW_ANONYMOUS_PASSWORD=anon
RUN cubicweb-ctl create frarchives_edition instance --automatic --no-db-create
# uncomment option so that cubicweb searches for it in the environment
RUN sed -i "s/^#published-index-name=/published-index-name=/" /etc/cubicweb.d/instance/all-in-one.conf
RUN sed -i "s/^#ead-services-dir=/ead-services-dir=/" /etc/cubicweb.d/instance/all-in-one.conf
RUN sed -i "s/^#=eac-services-dir/=eac-services-dir/" /etc/cubicweb.d/instance/all-in-one.conf
COPY pyramid.ini /etc/cubicweb.d/instance/pyramid.ini

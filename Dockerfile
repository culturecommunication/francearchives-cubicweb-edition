FROM python:3.9-alpine AS temp
RUN apk add --no-cache nodejs
RUN apk add --no-cache npm # or use python:3.9 image
RUN apk add build-base
RUN npm install -g sass
COPY . .

RUN npm ci
ENV NODE_ENV="production"
RUN npm run build
RUN python setup.py sdist

FROM francearchives/cubicweb-francearchives:2.21.4
ENV CW_INSTANCE=instance
COPY ./requirements.txt /requirements.txt
RUN pip install --no-cache-dir -r /requirements.txt
COPY --from=temp dist/cubicweb-frarchives-edition-*.tar.gz .
# bump version due to incompatibility w/ 9.4.0
# see https://github.com/linkchecker/linkchecker/tree/v10.0.0
# and https://github.com/linkchecker/linkchecker/tree/v10.0.1 for details
RUN pip install beautifulsoup4==4.8.0
RUN pip install Linkchecker==10.0.1
RUN pip install cubicweb-frarchives-edition-*.tar.gz
RUN pip install pyramid-session-redis
ENV PATH=".local/bin:$PATH"
USER cubicweb
ENV CUBE=frarchives_edition
RUN docker-cubicweb-helper create-instance

# FIXME https://forge.extranet.logilab.fr/cubicweb/cubicweb/-/issues/468
RUN echo 'superuser-login=' >> /etc/cubicweb.d/instance/sources
RUN echo 'superuser-password=' >> /etc/cubicweb.d/instance/sources
USER root
RUN rm /requirements.txt
USER cubicweb

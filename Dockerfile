# syntax=docker/dockerfile:1
FROM            node:14.15.3-alpine as ngrok
# https://github.com/shkoliar/docker-ngrok/blob/master/Dockerfile

RUN             apk add --no-cache --virtual .bootstrap-deps ca-certificates && \
                wget -O /tmp/ngrok.zip https://bin.equinox.io/c/4VmDzA7iaHb/ngrok-stable-linux-amd64.zip && \
                unzip -o /tmp/ngrok.zip -d / && \
                apk del .bootstrap-deps && \
                rm -rf /tmp/* && \
                rm -rf /var/cache/apk/*

FROM node:14.15.3-alpine as builder

LABEL maintainer="Martin Minka"

WORKDIR /opt/app

# add Nightscout
ADD app/cgm-remote-monitor /opt/app
RUN npm install --only=production --cache /tmp/empty-cache && \
    npm run postinstall
RUN mkdir tmp && chown node:node tmp

# add storage plugins
ADD packages /opt/packages/

RUN cd /opt/packages/nightscout-storage-basic && npm install --only=production --cache /tmp/empty-cache && \
    cd /opt/packages/nightscout-storage-sqlite && npm install --only=production --cache /tmp/empty-cache && \
    cd /opt/app && npm i ../packages/nightscout-storage-basic && npm i ../packages/nightscout-storage-sqlite

FROM node:14.15.3-alpine

RUN npm install pm2 -g

ENV API_SECRET mysecrettoken
ENV HOSTNAME 0.0.0.0
ENV ENABLE "dbsize careportal cage basal bolus iob sage treatmentnotify rawbg alexa cors basalprofile pushover bgi iage cob food direction bage upbat googlehome boluscalc bwp speech bridge"
ENV SHOW_PLUGINS "rawbg-on careportal upbat iob profile cage cob basal avg treatments boluscalc pump openaps iage speech"
ENV DISABLE "cage iage bage upbat bridge"
ENV DISPLAY_UNITS "mmol"
ENV TIME_FORMAT 24
ENV ALARM_TYPES "predict"
ENV LANGUAGE en
ENV INSECURE_USE_HTTP true
ENV PORT 1337
ENV NODE_ENV production
ENV AUTH_FAIL_DELAY 50

WORKDIR /opt/app
COPY --from=ngrok /ngrok /usr/local/bin/ngrok
COPY --from=builder /opt ../

#USER node
EXPOSE 1337
EXPOSE 4551

CMD ["pm2-runtime", "lib/server/server.js"]

# needs to be set to anything to bootstrap storage engine in Nightscout
ENV CUSTOMCONNSTR_mongo custom://
ENV STORAGE_CLASS @nightscout-storage-sqlite
ENV STORAGE_SQLITE_DB /mnt/nightscout.sqlite
ENV STORAGE_SQLITE_LOGFILE /mnt/storage-sqlite.log

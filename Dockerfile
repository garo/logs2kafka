FROM    node:6.2.0
WORKDIR /logs2kafka
COPY    package.json /logs2kafka/
RUN     npm install
COPY    . /logs2kafka
CMD     ["npm", "start"]


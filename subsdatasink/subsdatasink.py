#!/usr/bin/env python
import os
import copy
import psycopg2

from datetime import datetime

from flask import Flask, jsonify, make_response, request
from flask.ext.restful import Api, Resource, reqparse
from flask.ext.httpauth import HTTPBasicAuth

import logging
import logging.handlers
log = logging.getLogger(__name__)


__author__ = "Raido Pahtma"
__license__ = "MIT"

version = "0.2.0"


class DataElement(object):

    def __init__(self, source, data_type, value, arrival=0, ps=0, pe=0):
        self.source = source
        self.type = data_type
        self.value = value
        self.arrival = arrival
        self.production_start = ps
        self.production_end = pe


def process_data(data):
    elements = []
    if data is not None:
        data = copy.deepcopy(data)

        if "source" in data and data["source"] is not None:
            source = data["source"]

            if "type" not in data:
                data["type"] = None

            if "timestamp_arrival" not in data:
                data["timestamp_arrival"] = None

            if "values" in data:
                values = data["values"]
            else:  # Make the single value look like the array
                values = [{}]
                if "value" in data:
                    values[0]["value"] = data["value"]
                if "timestamp_production" in data:
                    values[0]["timestamp_production"] = data["timestamp_production"]
                if "duration_production" in data:
                    values[0]["duration_production"] = data["duration_production"]

            for item in values:
                # apply default values for any optional elements
                if "value" not in item:
                    item["value"] = None
                if "timestamp_production" not in item:
                    item["timestamp_production"] = None
                if "duration_production" not in item:
                    item["duration_production"] = None

                production_start = item["timestamp_production"]
                if production_start is not None and item["duration_production"] is not None:
                    production_end = production_start + item["duration_production"]
                else:
                    production_end = production_start

                elements.append(DataElement(source, data["type"], item["value"],
                                            arrival=data["timestamp_arrival"],
                                            ps=production_start, pe=production_end))
        else:
            log.warning("no source in data")
    else:
        log.warning("data is None")

    return elements


class DataSinkPostgres(object):

    def __init__(self, host, port, user, password):
        self._host = host
        self._port = port
        self._user = user
        self._pass = password

    @staticmethod
    def _sql_timestamp(ts):
        if ts is None:
            return "NULL"
        else:
            return datetime.utcfromtimestamp(ts).strftime("'%Y-%m-%d %H:%M:%S.%f'")

    def store_data(self, database, data):
        # {
        #     "source": "0011223344556677",
        #     "type": "dt_some_data",
        #     "value": 0.001,
        #     "production_interval": 5.0,
        #     "timestamp_production": 1425661616.000,
        #     "timestamp_arrival": 1425661616.000
        # }

        # {
        #     "source": "0011223344556677",
        #     "type": "dt_some_data",
        #     "values": [
        #       {"timestamp_production": 1425661615.000, "production_interval": 1.0, "value": 0.001},
        #       {"timestamp_production": 1425661616.000, "production_interval": 1.0, "value": 0.002}
        #      ],
        #     "timestamp_arrival": 1425661616.000
        # }

        elements = process_data(data)

        if len(elements) > 0:
            try:
                db = psycopg2.connect(host=self._host, port=self._port, database=database, user=self._user, password=self._pass)

                try:
                    cursor = db.cursor()

                    s = "INSERT INTO data (guid, arrival, production_start, production_end, type, value) VALUES ('{:s}', {:s}, {:s}, {:s}, {:s}, {:s})"

                    for d in elements:

                        sql = s.format(d.source,
                                       self._sql_timestamp(d.arrival),
                                       self._sql_timestamp(d.production_start),
                                       self._sql_timestamp(d.production_end),
                                       "NULL" if d.type is None else "'{:s}'".format(d.type),
                                       "NULL" if d.value is None else "{:f}".format(d.value))

                        cursor.execute(sql)

                    db.commit()
                    db.close()
                    return 201

                except Exception:
                    log.exception("Database fail")
                    db.rollback()
                    db.close()
                    return 503

            except Exception:
                log.exception("Database fail")
                return 503

        else:
            return 406


class DataSinkFlask(Resource):
    auth = HTTPBasicAuth()

    def __init__(self, users, datasink):
        self._users = users
        self._datasink = datasink
        self.reqparse = reqparse.RequestParser()

        self.auth.get_password(self.get_password)  # Because auth decorator cannot handle self
        super(DataSinkFlask, self).__init__()

    def get_password(self, username):
        if username in self._users:
            return self._users[username]
        return None

    @staticmethod
    @auth.error_handler
    def unauthorized():
        # return 403 instead of 401 to prevent browsers from displaying the default
        # auth dialog
        return make_response(jsonify({"message": "Unauthorized access"}), 403)

    @auth.login_required
    def get(self, database):
        return make_response(jsonify({"message": "Just POST something..."}), 200)

    @auth.login_required
    def post(self, database):
        data = request.get_json(silent=True, force=True)

        log.debug("received {}".format(data))

        code = self._datasink.store_data(database, data)
        if 200 <= code < 300:
            response = jsonify({"code": code})
            response.status_code = code
        else:
            response = jsonify({"code": code, "request": data})
            response.status_code = code

        return response


def tornadoserver(app, host, port, ssl_context):
    import signal

    from tornado.wsgi import WSGIContainer
    from tornado.ioloop import IOLoop
    from tornado.httpserver import HTTPServer

    http_server = HTTPServer(WSGIContainer(app), ssl_options=ssl_context)
    http_server.listen(port, address=host)

    def shutdown():
        log.info("shutdown")
        http_server.io_loop.stop()

    def sig_handler(sig, frame):
        log.debug('Caught signal: %s', sig)
        IOLoop.instance().add_callback(shutdown)

    signal.signal(signal.SIGTERM, sig_handler)
    signal.signal(signal.SIGINT, sig_handler)

    IOLoop.instance().start()


def main():

    import argparse
    from argconfparse.argconfparse import ConfigArgumentParser, arg_str2bool
    parser = ConfigArgumentParser("subsdatasink", description="datasink arguments",
                                  formatter_class=argparse.ArgumentDefaultsHelpFormatter)

    parser.add_argument("--db-host", default="localhost")
    parser.add_argument("--db-port", default=5432)
    parser.add_argument("--db-user", default="datasink")
    parser.add_argument("--db-pass", default="datasink")

    parser.add_argument("--api-host", default="0.0.0.0")
    parser.add_argument("--api-port", type=int, default=54320)
    parser.add_argument("--api-user", default="user")
    parser.add_argument("--api-pass", default="pass")

    parser.add_argument("--logdir", default="/var/log/subsdatasink")

    parser.add_argument("--http", type=arg_str2bool, nargs="?", const=True, default=False)
    parser.add_argument("--server-crt", default="server.crt")
    parser.add_argument("--server-key", default="server.key")

    parser.add_argument("--debug", type=arg_str2bool, nargs="?", const=True, default=False,
                        help="Configure logging.basicConfig")
    parser.add_argument("--debug-server", type=arg_str2bool, nargs="?", const=True, default=False,
                        help="Run app with Flask debug server")

    args = parser.parse_args()

    if args.debug:
        logging.basicConfig(level=logging.DEBUG)
    else:
        if not os.path.isdir(args.logdir):
            os.makedirs(args.logdir)

        ch = logging.handlers.TimedRotatingFileHandler(os.path.join(args.logdir, "subsdatasink.log"), when="W6", backupCount=8)
        ch.setLevel(logging.DEBUG)

        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')

        ch.setFormatter(formatter)

        root_logger = logging.getLogger()
        root_logger.setLevel(logging.DEBUG)
        root_logger.addHandler(ch)

    if args.http:
        context = None
    else:
        import ssl
        # openssl genrsa -des3 -out server.key.secure 2048
        # openssl rsa -in server.key.secure -out server.key
        # openssl req -new -key server.key -out server.csr
        # openssl x509 -req -days 365 -in server.csr -signkey server.key -out server.crt

        context = ssl.SSLContext(ssl.PROTOCOL_TLSv1_2)
        context.load_cert_chain(args.server_crt, args.server_key)

    datasink = DataSinkPostgres(args.db_host, args.db_port, args.db_user, args.db_pass)

    app = Flask(__name__, static_url_path="")
    api = Api(app)
    api.add_resource(DataSinkFlask, '/api/v0/sink/<string:database>', endpoint='datasink',
                     resource_class_args=({args.api_user: args.api_pass}, datasink))

    if args.debug_server:
        app.run(debug=args.debug, host=args.api_host, port=args.api_port, ssl_context=context)
    else:
        tornadoserver(app, args.api_host, args.api_port, context)


if __name__ == '__main__':
    main()

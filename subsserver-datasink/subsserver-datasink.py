#!flask/bin/python

import psycopg2
import argparse

from datetime import datetime
from flask import Flask, jsonify, abort, make_response, request
from flask.ext.restful import Api, Resource, reqparse, fields, marshal
from flask.ext.httpauth import HTTPBasicAuth

import ssl
# openssl genrsa -des3 -out server.key.secure 2048
# openssl rsa -in server.key.secure -out server.key
# openssl req -new -key server.key -out server.csr
# openssl x509 -req -days 365 -in server.csr -signkey server.key -out server.crt

import logging
log = logging.getLogger(__name__)


__author__ = "Raido Pahtma"
__license__ = "MIT"


class DataSinkPostgres(object):

    def __init__(self, host, port, user, password):
        self._host = host
        self._port = port
        self._user = user
        self._pass = password

    def store_data(self, database, data):
        # {
        #     "source": "0011223344556677",
        #     "type": "dt_some_data",
        #     "value": 0.001, OR "value": [0.001, 0.002],
        #     "period": 100,
        #     "timestamp_production": 1425661616.000,
        #     "timestamp_arrival": 1425661616.000
        # }

        if "source" not in data or data["source"] is None:
            log.warning("no source in data")
            return False

        try:
            db = psycopg2.connect(host=self._host, port=self._port, database=database, user=self._user, password=self._pass)

            try:
                cursor = db.cursor()

                if "timestamp_arrival" not in data or data["timestamp_arrival"] is None:
                    t_arrival = "NULL"
                else:
                    t_arrival = datetime.utcfromtimestamp(data["timestamp_arrival"]).strftime("'%Y-%m-%d %H:%M:%S.%f'")

                if "timestamp_production" not in data or data["timestamp_production"] is None:
                    t_production_end = None
                else:
                    t_production_end = data["timestamp_production"]

                if "period" not in data or data["period"] is None:
                    data_period = None
                else:
                    data_period = data["period"]

                if t_production_end is None:
                    t_production_start = None
                elif data_period is None:
                    t_production_start = t_production_end
                else:
                    t_production_start = t_production_end - data_period

                if "type" not in data or data["type"] is None:
                    data_type = "NULL"
                else:
                    data_type = "'{:s}'".format(data["type"])

                dv = []
                if "value" not in data or data["value"] is None:
                    dv.append((None, t_production_start, t_production_end))
                else:
                    if isinstance(data["value"], list) and len(data["value"]) > 0:
                        if t_production_end is not None and data_period is not None:
                            t_start = t_production_end - data_period
                            period = data_period / len(data["value"])
                        else:
                            t_start = None
                            period = None

                        for i in xrange(0, len(data["value"])):
                            if t_start is not None:
                                ts = t_start + i*period
                                te = ts + period
                            else:
                                ts = None
                                te = None
                            dv.append((data["value"][i], ts, te))
                    else:
                        dv.append((data["value"], t_production_start, t_production_end))

                s = "INSERT INTO data (guid, arrival, production_start, production_end, type, value) VALUES ('{:s}', {:s}, {:s}, {:s}, {:s}, {:s})"

                for idx in xrange(0, len(dv)):
                    v = "NULL" if dv[idx][0] is None else "{:f}".format(dv[idx][0])
                    ts = "NULL" if dv[idx][1] is None else datetime.utcfromtimestamp(dv[idx][1]).strftime("'%Y-%m-%d %H:%M:%S.%f'")
                    te = "NULL" if dv[idx][2] is None else datetime.utcfromtimestamp(dv[idx][2]).strftime("'%Y-%m-%d %H:%M:%S.%f'")
                    sql = s.format(data["source"], t_arrival, ts, te, data_type, v)

                    log.debug("sql: {}".format(sql))

                    cursor.execute(sql)
                    db.commit()

                return True

            except Exception:
                log.exception("Database fail")
                db.rollback()

            db.close()

        except Exception:
            log.exception("Database fail")

        return False


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
        return make_response(jsonify({"message": "Just POST something..."}), 400)

    @auth.login_required
    def post(self, database):
        data = request.get_json()

        log.debug("received {}".format(data))

        if self._datasink.store_data(database, data):
            response = jsonify({"code": 201})
            response.status_code = 201
            return response

        return data, 400


def main():

    parser = argparse.ArgumentParser("datasink", description="datasink arguments", formatter_class=argparse.ArgumentDefaultsHelpFormatter)

    parser.add_argument("--db-host", default="localhost")
    parser.add_argument("--db-port", default=5432)
    parser.add_argument("--db-user", required=True)
    parser.add_argument("--db-pass", required=True)

    parser.add_argument("--api-host", default="0.0.0.0")
    parser.add_argument("--api-port", type=int, default=54320)
    parser.add_argument("--api-user", default="user")
    parser.add_argument("--api-pass", default="pass")

    parser.add_argument("--http", action="store_true")
    parser.add_argument("--server-crt", default="server.crt")
    parser.add_argument("--server-key", default="server.key")

    parser.add_argument("--debug", action="store_true")

    args = parser.parse_args()

    if args.debug:
        logging.basicConfig(level=logging.DEBUG)

    if args.http:
        context = None
    else:
        context = ssl.SSLContext(ssl.PROTOCOL_TLSv1_2)
        context.load_cert_chain(args.server_crt, args.server_key)

    datasink = DataSinkPostgres(args.db_host, args.db_port, args.db_user, args.db_pass)

    app = Flask(__name__, static_url_path="")
    api = Api(app)
    api.add_resource(DataSinkFlask, '/api/v0/sink/<string:database>', endpoint='datasink',
                     resource_class_args=({args.api_user: args.api_pass}, datasink))
    app.run(debug=args.debug, host=args.api_host, port=args.api_port, ssl_context=context)


if __name__ == '__main__':
    main()

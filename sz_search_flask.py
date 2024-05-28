#! /usr/bin/env python3

from flask import Flask,request,jsonify

import concurrent.futures
import argparse
import sys
import os

from senzing import G2Engine, G2Exception, G2EngineFlags, G2Diagnostic

executor = None
engine = None

app = Flask(__name__)

def process_search(engine, search_json, engine_flags, profile):
    try:
        response = bytearray()
        engine.searchByAttributesV3( search_json, profile, response, flags=engine_flags )
        return response.decode()
    except Exception as err:
        print(f"{err} [{search_json}]", file=sys.stderr)
        raise

@app.route('/search', methods = ['POST'])
def do_search():
    global executor
    global engine
    user_request = request.data.decode()
    flags = G2EngineFlags.G2_SEARCH_BY_ATTRIBUTES_DEFAULT_FLAGS
    if request.args.get('flags'):
        user_flags = request.args.get('flags').split('|')
        flags = int(G2EngineFlags.combine_flags(user_flags))
    try:
        task = executor.submit(process_search, engine, user_request, flags, request.args.get('profile'))
        return task.result()
    except Exception as err:
        return jsonify({'error':str(err)}), 500


try:
  with app.app_context():
    engine_config = os.getenv("SENZING_ENGINE_CONFIGURATION_JSON")
    if not engine_config:
        print(
            "The environment variable SENZING_ENGINE_CONFIGURATION_JSON must be set with a proper JSON configuration.",
            file=sys.stderr,
        )
        print(
            "Please see https://senzing.zendesk.com/hc/en-us/articles/360038774134-G2Module-Configuration-and-the-Senzing-API",
            file=sys.stderr,
        )
        exit(-1)

    # Initialize the G2Engine
    engine = G2Engine()
    engine.init("sz_search_perftest", engine_config, False)
    engine.primeEngine()

    max_workers = int(os.getenv("SENZING_THREADS_PER_PROCESS", 0))
    if not max_workers:  # reset to null for executors
        max_workers = None
    executor = concurrent.futures.ThreadPoolExecutor(max_workers)

except Exception as err:
    print(err, file=sys.stderr)
    exit(-1)

if __name__ == '__main__':
    app.run(debug=False,host='0.0.0.0',port=5000,threaded=True)



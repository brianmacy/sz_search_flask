#! /usr/bin/env python3

import concurrent.futures

import argparse
import pathlib
import orjson
import itertools
import requests

import sys
import os
import time
from timeit import default_timer as timer

INTERVAL = 1000


def process_line(url, line):
    try:
        record = orjson.loads(line.encode())
        startTime = timer()
        response = requests.post(url, data=line)
        return (timer()-startTime,record["RECORD_ID"])
    except Exception as err:
        print(f"{err} [{line}]", file=sys.stderr)
        raise


try:
    parser = argparse.ArgumentParser()
    parser.add_argument("fileToProcess", default=None)
    parser.add_argument("-u", "--url", dest="url", required=True, default=None)
    parser.add_argument(
        "-t",
        "--debugTrace",
        dest="debugTrace",
        action="store_true",
        default=False,
        help="output debug trace information",
    )
    args = parser.parse_args()

    max_workers = int(os.getenv("SENZING_THREADS_PER_PROCESS", 0))
    if not max_workers:  # reset to null for executors
        max_workers = None

    beginTime = prevTime = time.time()
    timeMin = timeMax = timeTot = count = 0;
    timesAll = []

    with open(args.fileToProcess, "r") as fp:
        numLines = 0
        q_multiple = 2

        with concurrent.futures.ThreadPoolExecutor(max_workers) as executor:
            print(f"Searching with {executor._max_workers} threads")
            try:
                futures = {
                    executor.submit(process_line, args.url, line): line
                    for line in itertools.islice(fp, q_multiple * executor._max_workers)
                }

                while futures:

                    done, _ = concurrent.futures.wait(
                        futures, return_when=concurrent.futures.FIRST_COMPLETED
                    )
                    for fut in done:
                        result = fut.result()
                        futures.pop(fut)

                        if result:
                            count += 1
                            result_time = result[0]
                            timesAll.append(result)
                            timeTot += result_time
                            if timeMin == 0:
                                timeMin = result_time
                            else:
                                timeMin = min(timeMin,result_time)
                            timeMax = max(timeMax,result_time)

                        numLines += 1
                        if numLines % INTERVAL == 0:
                            nowTime = time.time()
                            speed = int(INTERVAL / (nowTime - prevTime))
                            print(
                                    f"Processed {numLines} searches, {speed} records per second: avg[{timeTot/count:.3f}s] tps[{count/(time.time()-beginTime):.3f}/s] min[{timeMin:.3f}s] max[{timeMax:.3f}s]"
                            )
                            prevTime = nowTime

                        line = fp.readline()
                        if line:
                            futures[executor.submit(process_line, args.url, line)] = line

                print(f"Processed total of {numLines} searches: avg[{timeTot/count:.3f}s] tps[{count/(time.time()-beginTime):.3f}/s] min[{timeMin:.3f}s] max[{timeMax:.3f}s]")
                timesAll.sort(key=lambda x: x[0], reverse=True)

                i = 0
                while i<count:
                    if timesAll[i][0] <= 1.0:
                        break
                    i += 1
                print(f"Percent under 1s: {(count-i)/count*100:.1f}%")
                print(f"longest: {timesAll[0][0]:.3f}s record[{timesAll[0][1]}]")

                p99 = int(count*.01)
                p95 = int(count*.05)
                p90 = int(count*.10)

                i = 0
                while i<p90:
                    i += 1
                    if i == p99:
                        print(f"p99: {timesAll[i][0]:.3f}s record[{timesAll[i][1]}]")
                    if i == p95:
                        print(f"p95: {timesAll[i][0]:.3f}s record[{timesAll[i][1]}]")
                    if i == p90:
                        print(f"p90: {timesAll[i][0]:.3f}s record[{timesAll[i][1]}]")

            except Exception as err:
                print(f"Shutting down due to error: {err}", file=sys.stderr)
                executor.shutdown()
                exit(-1)

except Exception as err:
    print(err, file=sys.stderr)
    exit(-1)


import datetime
import gzip
import json
import os

from warren.warren import Warren
import config

gz = None
archive_dir = config.get("options", "archive-dir")


def sanitize_dict(d):
    ret = {}
    for x in d:
        if '.' not in x:
            ret[x] = d[x]
        else:
            sp = x.split('.')
            val = ret
            for y in sp[:-1]:
                val[y] = {} if y not in val else val[y]
                val = val[y]
            val[sp[-1]] = d[x]
    return ret


def open_file():
    global gz
    global archive_dir

    now = datetime.datetime.now(tz=datetime.timezone.utc)

    ys = "{y:04d}".format(y=now.year)
    ms = "{y:02d}".format(y=now.month)
    ds = "{y:02d}".format(y=now.day)
    hs = "{y:02d}".format(y=now.hour)

    filename = ys + "-" + ms + "-" + ds + "." + hs + ".json.gz"

    if gz is None or gz.name[0][-21:] != filename:
        try:
            os.mkdir(archive_dir + "/" + ys)
        except FileExistsError:
            pass

        try:
            os.mkdir(archive_dir + "/" + ys + "/" + ms)
        except FileExistsError:
            pass

        try:
            os.mkdir(archive_dir + "/" + ys + "/" + ms + "/" + ds)
        except FileExistsError:
            pass

        if gz is not None:
            gz.close()

        gz = gzip.open(archive_dir + "/" + ys + "/" + ms + "/" + ds + "/" + filename, "ab")
        return gz
    else:
        return gz


def message_cb(data):
    global gz

    open_file()
    jo = json.loads(data["message"])

    if jo["category"] != "heartbeat":
        jo["collector"] = data["queue"]
        if config.getboolean(data["queue"], "rewrite-dest"):
            jo["destination-ip"] = config.get(data["queue"], "rewrite-dest-ip")

        jo = sanitize_dict(jo)
        gz.write(json.dumps(jo).encode() + b"\n")

    else:
        print("heartbeat from " + data["queue"])


def main():
    warren = Warren(config.get("options", "amqp-url"), message_cb)
    warren.start_connection()

    for x in config.getqueues():
        warren.add_queue(x)

    try:
        warren.start_consuming()
    except KeyboardInterrupt:
        gz.close()


if __name__ == "__main__":
    main()

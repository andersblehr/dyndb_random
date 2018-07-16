#!/usr/bin/env python

from __future__ import print_function

import csv
import datetime
import dateutil.parser
import getopt
import json
import os
import random
import sys
import uuid

import geohash
import loremipsum


def usage(exit_code=0):
    print("Generate random test data for a DynamoDB table.")
    print("")
    print("Usage:")
    print("  " + os.path.basename(__file__) + " -t <table> [-n <num rows>] [-o <file>] [-d] [-c] [-x]")
    print("")
    print("Options:")
    print("  -t: Generate data for DynamoDB table with name <table>. A value")
    print("      definitions file <table>.vdef.json must be present in the local")
    print("      directory or in $SHOTRUN_HOME/test/data/vdef.")
    print("  -n: Generate <num rows> rows of data (default: 10).")
    print("  -o: Write data to <file>.json (default: ./<table>[.<timestamp>].json).")
    print("  -d: Write data in DynamoDB's internal JSON format (default: plain JSON).")
    print("  -c: Also write data to CSV file (./<table>[.timestamp].csv), or to")
    print("      <file>.csv if the -o option has been given.")
    print("  -x: Do not link tables.")
    print("  -h: Show this help text.")
    print("")
    print("Requires:")
    print("  loremipsum (http://pythonhosted.org/loremipsum/)")
    print("  python-geohash (https://github.com/hkwi/python-geohash)")

    sys.exit(exit_code)


def epoch_seconds(isodate=None):
    if not isodate:
        isodate = datetime.datetime.utcnow().isoformat()
    
    datetime_isodate = dateutil.parser.parse(isodate)
    datetime_zero = datetime.datetime(1970, 1, 1)
    
    return int((datetime_isodate - datetime_zero).total_seconds())


def load_value_defs(table):
    vdef_file_name = os.path.join(table + '.vdef.json')

    if not os.path.isfile(vdef_file_name):
        vdef_file_name = os.path.join(vdef_dir, vdef_file_name)

    if not os.path.isfile(vdef_file_name):
        print("No such file: " + vdef_file_name)
        sys.exit(1)
        
    with open(vdef_file_name) as vdef_file:
        value_defs = json.load(vdef_file)

    return value_defs


def load_linked_table(table, key):
    global linked_tables
    
    table_value_defs = load_value_defs(table)
    table_column_order = table_value_defs['column_order']
    table_key_index = table_column_order.index(key)
    data_file_name = table + '.csv'
    
    if not os.path.isfile(data_file_name):
        data_file_name = os.path.join(csv_dir, data_file_name)

        if not os.path.isfile(data_file_name):
            print("Cannot find linked data file: " + data_file_name)
            sys.exit(1)

    linkable_keys = []
        
    with open(data_file_name) as data_file:
        header_row = True
        data_reader = csv.reader(data_file)

        for row in data_reader:
            if not header_row:
                linkable_keys.append(row[table_key_index])
            else:
                header_row = False

    linked_tables[table] = linkable_keys

    return linkable_keys


def pick_linked_key(table, key):
    if table in linked_tables:
        linked_keys = linked_tables[table]
    else:
        linked_keys = load_linked_table(table, key)
        
    return linked_keys[random.randrange(0, len(linked_keys))]


def generate_lorem(word_range):
    min_words, max_words = (1, 100)

    if word_range:
        min_words, max_words = word_range.partition(':')[::2]

        min_words = int(min_words)
        max_words = int(max_words) if max_words else 1000

    ignore, words, lorem = loremipsum.generate_sentence()

    while words < int(min_words) or words > int(max_words):
        ignore, words, lorem = loremipsum.generate_sentence()

    return lorem


def generate_date(date_range, row, column_order):
    ref_date_column, rel_range = date_range.split('+')
    rel_range_min, rel_range_max = rel_range.split(':')
    rel_range_min = int(rel_range_min)
    rel_range_max = int(rel_range_max)

    if ref_date_column == '#now':
        ref_date_seconds = epoch_seconds()
    else:
        ref_date = row[column_order.index(ref_date_column)]
        ref_date_seconds = epoch_seconds(ref_date)

    date_min_seconds = ref_date_seconds + 86400 * rel_range_min
    date_max_seconds = ref_date_seconds + 86400 * rel_range_max
    date_seconds = random.randrange(date_min_seconds, date_max_seconds)

    return datetime.datetime.fromtimestamp(date_seconds).isoformat()
    

def pick_enum(enums):
    enums = enums.split(',')

    return enums[random.randrange(0, len(enums))]


def generate_geo_hash(hash_length, row, column_order):
    hash_length = int(hash_length)
    lat = row[column_order.index('lat')]
    lon = row[column_order.index('long')]

    return geohash.encode(lat, lon)[:hash_length]


def generate_int(value_range, row, column_order):
    ref_value = 0
    
    if '+' in value_range:
        ref_column, value_range = value_range.split('+')
        ref_value = int(row[column_order.index(ref_column)])

    value_min, value_max = value_range.split(':')
    value_min = ref_value + int(value_min)
    value_max = ref_value + int(value_max)

    return random.randrange(value_min, value_max)


def generate_float(value_range):
    value_min, value_max = value_range.split(':')
    value_min = float(value_min)
    value_max = float(value_max)

    return random.uniform(value_min, value_max)


def generate_value(column_def, row, column_order):
    column_type = column_def['type']
    column_format = column_def['format'] if 'format' in column_def else None
    column_range = column_def['range'] if 'range' in column_def else None

    if link_tables and 'linked' in column_def:
        table, key = column_def['linked'].split(':')
        value = pick_linked_key(table, key)
    elif column_type == 'S':
        if column_format == 'uuid':
            value = str(uuid.uuid4())
        elif column_format == 'lorem':
            value = generate_lorem(column_range)
        elif column_format == 'date':
            value = generate_date(column_range, row, column_order)
        elif column_format == 'enum':
            value = pick_enum(column_range)
        elif column_format == 'geohash':
            value = generate_geo_hash(column_range, row, column_order)
        elif column_format == 'value':
            value = column_range
    elif column_type == 'N':
        if column_format == 'int':
            value = generate_int(column_range, row, column_order)
        elif column_format == 'float':
            value = generate_float(column_range)
        elif column_format == 'value':
            value = column_range
    elif column_type == 'L':
        value = []
        
        item_def = column_def['item']
        min_items, max_items = column_range.split(':')
        num_items = random.randrange(int(min_items), int(max_items) + 1)

        for i in range(0, num_items):
            list_item = generate_value(item_def, row, column_order)

            if not list_item in value:
                value.append(list_item)

    return value


def generate_row(value_defs, column_order):
    row = []
    
    for column_name in column_order:
        column_def = value_defs['columns'][column_name]

        row.append(generate_value(column_def, row, column_order))

    return row


def db_item(row, value_defs):
    column_order = value_defs['column_order']
    item = {}

    for column_name in column_order:
        column_def = value_defs['columns'][column_name]
        column_type = column_def['type']
        column_index = column_order.index(column_name)

        if column_type == 'L':
            keys = row[column_index]
            typed_keys = []

            for key in keys:
                typed_keys.append({ "S": key })

            item[column_name] = { column_type: typed_keys }
        else:
            item[column_name] = { column_type: str(row[column_index]) }

    return { "PutRequest": { "Item": item } }


def json_item(row, column_order):
    item = {}

    for i in range(0, len(column_order)):
        item[column_order[i]] = row[i]

    return item


def generate_data(table, num_rows):
    global output_file_name
    
    value_defs = load_value_defs(table)
    column_order = value_defs['column_order']
    json_items = []

    if write_csv:
        csv_rows = [column_order]
    
    while len(json_items) < num_rows + 1:
        row = generate_row(value_defs, column_order)

        if write_db_json:
            json_items.append(db_item(row, value_defs))
        else:
            json_items.append(json_item(row, column_order))

        if write_csv:
            csv_rows.append(row)

    if write_db_json:
        json_items = { table: json_items }

    if not output_file_name:
        output_file_name = table
        
    json_file_name = output_file_name + '.json'

    if os.path.isfile(json_file_name):
        timestamp = datetime.datetime.now().isoformat()
        json_file_name = output_file_name + '.' + timestamp + '.json'

    with open(json_file_name, 'w') as json_file:
        json.dump(json_items, json_file, indent=4)

    if write_db_json:
        print("DymanoDB JSON written to: " + json_file_name)
    else:
        print("JSON data written to: " + json_file_name)

    if write_csv:
        csv_file_name = table + '.csv'
        
        if os.path.isfile(csv_file_name):
            if not timestamp:
                timestamp = datetime.datetime.now().isoformat()
            
            csv_file_name = table + '.' + timestamp + '.csv'

        with open(csv_file_name, 'w') as csv_file:
            output_csv_writer = csv.writer(csv_file)
            output_csv_writer.writerows(csv_rows)

        print("CSV data written to: " + csv_file_name)


def main(argv):
    global output_file_name
    global write_db_json
    global write_csv
    global link_tables
    
    num_rows = 10

    try:
        opts, args = getopt.getopt(argv, 't:n:o:dcxh')
    except getopt.GetoptError:
        usage(2)

    if len(opts) > 0:
        for opt, arg in opts:
            if opt == '-h':
                usage()
            elif opt == '-t':
                table = arg
            elif opt == '-n':
                num_rows = int(arg)
            elif opt == '-o':
                output_file_name = arg + '.json'
            elif opt == '-d':
                write_db_json = True
            elif opt == '-c':
                write_csv = True
            elif opt == '-x':
                link_tables = False
            else:
                usage(2)
    else:
        usage(2)

    if not table:
        usage(2)

    random.seed()
    generate_data(table, num_rows)
    

if __name__ == '__main__':
    base_dir = os.environ['SHOTRUN_HOME']
    vdef_dir = os.path.join(base_dir, 'test/data/vdef')
    csv_dir = os.path.join(base_dir, 'test/data/csv')
    
    output_file_name = ''
    write_db_json = False
    write_csv = False
    link_tables = True
    linked_tables = {}

    main(sys.argv[1:])

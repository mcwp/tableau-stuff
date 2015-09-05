"""
Extract certain fields from a csv file and create a new csv.
"""
import optparse
import sys
import os
import csv
from decimal import Decimal, ROUND_HALF_UP
import logging
log = logging.getLogger(__name__)
from datetime import datetime


class GetDataSet(object):
    """Extract certain fields from a csv file and create a new csv.
    """

    def __init__(self, csv_filename, date_start=None, date_end=None):
        self.fieldnames_in = None
        self.fieldnames_out = None
        self.csv_filename = csv_filename
        self.new_csv_name = None
        self.list_of_dicts = None
        self.get_rows = self.get_csv_bits
        self.date_start = datetime.strptime(
            date_start, '%Y-%m-%d').date() if date_start else None
        self.date_end = datetime.strptime(
            date_end, '%Y-%m-%d').date() if date_end else None

    def get_csv_bits(self):
        """Yield some rows from a csv file.
        """
        with open(self.csv_filename, 'rb') as f:
            reader = csv.DictReader(f)
            try:
                for row in reader:
                    if self.keep_me(row):
                        x = {k: row[k] for k in self.fieldnames_in}
                        yield x
            except csv.Error as e:
                sys.exit('line %d: %s' % (reader.line_num, e))

    def keep_me(self, row):
        """Keep this row?  Only in the date range, if given.
        Refactor:
            - if self.date_start, try to check it and return False only if out of range, raise if missing date fields
            - put row[c] in try/except KeyError and raise error if checking missing fields
            - keep check for null contents of present field
        """
        try:
            for c in (self.fieldnames_in + ['Date']):
                if not row[c]:
                    log.info('Skipping row with null %r', c)
                    return False
            row_date = datetime.strptime(row['Date'], '%Y-%m-%d').date()
            if (row_date < self.date_start or
                    row_date >= self.date_end):
                return False
        except KeyError:
            log.error("There is no %s in this data." % c)
            raise
        else:
            return True

    def write_new_csv(self):
        """Write out the new csv with only the given fieldnames_out.
        """
        with open(self.new_csv_name, 'wb') as new_csv:
            writer = csv.DictWriter(new_csv, self.fieldnames_out)
            # write the header row out first
            writer.writerow(dict(zip(self.fieldnames_out, self.fieldnames_out)))
            self.save_list_of_dicts()
            for row in self.list_of_dicts:
                writer.writerow(row)
        return(len(self.list_of_dicts))
        # return the length of the new file
        # and then print that out with self.new_csv_name

    def save_list_of_dicts(self):
        """Instead of writing out the new csv, maybe we need to save it as a
        list, for subsequent processing or whatever.
        """
        self.list_of_dicts = list()
        for row in self.get_rows():
            self.list_of_dicts.append(row)
        return self.list_of_dicts


class SimpleSubset(GetDataSet):
    """Create a data set with a fixed subset of the columns from the
    assumed columns in the input file.  Useful for debugging.  One can
    create an instance, overwrite the fieldnames (in/out), and then
    call the write_new_csv method.
    """
    def __init__(self, csv_filename, date_start, date_end):
        super(SimpleSubset, self).__init__(csv_filename, date_start, date_end)
        self.fieldnames_in = ['Name', 'Gross']
        self.fieldnames_out = self.fieldnames_in
        self.new_csv_name = os.path.splitext(self.csv_filename)[0] + '-new.csv'

    def keep_me(self, row):
        """Could be slightly different.
        """
        return super(SimpleSubset, self).keep_me(row)

def make_simple_subset(csv_filename, start_date, end_date):
    """Do a trivial test given a known csv.
    """
    data_set = SimpleSubset(csv_filename, start_date, end_date)
    lines = data_set.write_new_csv()
    print "wrote %r lines to %r" % (lines, data_set.new_csv_name)


def main():
    """Run the script."""
    usage = """usage %prog arg1

    arg1 is the name of the csv file to read.
    """
    # just for easy testing
    DATE_START = '2015-01-01'  # inclusive
    DATE_END = '2016-01-01'    # exclusive

    parser = optparse.OptionParser(usage=usage)
    parser.add_option(
        '-s',
        '--start_date',
        default=DATE_START,
        help='Inclusive start date.',
    )
    # parser.add_option(
    #     '-n',
    #     '--note',
    #     action='store_true',
    #     dest='note',
    #     help='Show report notes.',
    # )
    parser.add_option(
        '-e',
        '--end_date',
        default=DATE_END,
        help='Exclusive end date.',
    )
    (opts, args) = parser.parse_args()
    # if opts.note:
    #     show_notes()
    #     print parser.format_help()
    #     exit()

    if not args:
        print parser.format_help()
        exit()
        # raise optparse.BadOptionError('CSV file name required.')

    make_simple_subset(args[0], opts.start_date, opts.end_date)


if __name__ == '__main__':
    logging.basicConfig(level=logging.DEBUG)
    main()
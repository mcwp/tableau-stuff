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
        """Keep this row?
        """
        try:
            for c in (['Name'] + ['Gross']):
                if not row[c]:
                    log.info('Skipping row with null %r', c)
                    return False
            # row_date = datetime.strptime(row['Date'], '%Y-%m-%d').date()
            # if (row_date < self.date_start or
            #         row_date >= self.date_end):
            #     return False
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

class PayPalTransactions(GetDataSet):
    """Translate a csv file created by exporting transactions from
    Paypal into a csv file that conforms to the NPSP Data Import Template
    in the NPSP version of Salesforce.
    """

    PAYPAL_COLUMNS = [
        'Date',
        'Time',
        'Time Zone',
        'Name',
        'Type',
        'Status',
        'Subject',
        'Currency',
        'Gross',
        'Fee',
        'Net',
        'Note',
        'From Email Address',
        'To Email Address',
        'Transaction ID',
        'Payment Type',
        'Shipping address',
        'Address Status',
        'Item Title',
        'Item ID',
        'Shipping and Handling Amount',
        'Insurance Amount',
        'Sales Tax',
        'Option 1 Name',
        'Option 1 Value',
        'Option 2 Name',
        'Option 2 Value',
        'Auction Site',
        'Buyer ID',
        'Item URL',
        'Closing Date',
        'Reference Txn ID',
        'Invoice Number',
        'Subscription Number',
        'Custom Number',
        'Receipt ID',
        'Available Balance',
        'Address Line 1',
        'Address Line 2/District',
        'Town/City',
        'State/Province',
        'Zip/Postal Code',
        'Country',
        'Contact Phone Number',
        'Balance Impact',
    ]

    NPSP_COLUMNS = [
        'Contact1 Salutation',
        'Contact1 First Name',
        'Contact1 Last Name',
        'Contact1 Birthdate',
        'Contact1 Title',
        'Contact1 Personal Email',
        'Contact1 Work Email',
        'Contact1 Alternate Email',
        'Contact1 Preferred Email',
        'Contact1 Home Phone',
        'Contact1 Work Phone',
        'Contact1 Mobile Phone',
        'Contact1 Other Phone',
        'Contact1 Preferred Phone',
        'Contact2 Salutation',
        'Contact2 First Name',
        'Contact2 Last Name',
        'Contact2 Birthdate',
        'Contact2 Title',
        'Contact2 Personal Email',
        'Contact2 Work Email',
        'Contact2 Alternate Email',
        'Contact2 Preferred Email',
        'Contact2 Home Phone',
        'Contact2 Work Phone',
        'Contact2 Mobile Phone',
        'Contact2 Other Phone',
        'Contact2 Preferred Phone',
        'Account1 Name',
        'Account1 Street',
        'Account1 City',
        'Account1 State/Province',
        'Account1 Zip/Postal Code',
        'Account1 Country',
        'Account1 Phone',
        'Account1 Website',
        'Account2 Name',
        'Account2 Street',
        'Account2 City',
        'Account2 State/Province',
        'Account2 Zip/Postal Code',
        'Account2 Country',
        'Account2 Phone',
        'Account2 Website',
        'Home Street',
        'Home City',
        'Home State/Province',
        'Home Zip/Postal Code',
        'Home Country',
        'Donation Donor',
        'Donation Amount',
        'Donation Date',
        'Donation Name',
        'Donation Record Type Name',
        'Donation Stage',
        'Donation Type',
        'Donation Description',
        'Donation Member Level',
        'Donation Membership Start Date',
        'Donation Membership End Date',
        'Donation Membership Origin',
        'Donation Campaign Name',
        'Payment Check/Reference Number',
        'Payment Method',
    ]

    def __init__(self, csv_filename, date_start, date_end):
        super(PayPalTransactions, self).__init__(csv_filename, date_start, date_end)
        self.fieldnames_in = self.PAYPAL_COLUMNS
        self.fieldnames_out = self.NPSP_COLUMNS
        self.new_csv_name = os.path.splitext(self.csv_filename)[0] + '-npsp.csv'
        self.get_rows = self.get_paypal_rows

    def keep_me(self, row):
        """Could be slightly different.
        """
        return super(PayPalTransactions, self).keep_me(row)

    def get_paypal_rows(self):
        """Yield one dict output row at a time.
        """
        for row in self.get_csv_bits():
            output_row = dict.fromkeys(self.NPSP_COLUMNS, '')
            output_row['Donation Date'] = row['Date']
            first, last = row['Name'].rsplit(' ', 1)
            output_row['Contact1 First Name'] = first
            output_row['Contact1 Last Name'] = last
            output_row['Donation Type'] = row['Type']
            output_row['Donation Amount'] = row['Gross']
            output_row['Contact1 Personal Email'] = row['From Email Address']
            output_row['Donation Description'] = row['Note']
            output_row['Home Street'] = row['Address Line 1']
            if row['Address Line 2/District']:
                output_row['Home Street'] += ', ' + row['Address Line 2/District']
            output_row['Home City'] = row['Town/City']
            output_row['Home State/Province'] = row['State/Province']
            output_row['Home Zip/Postal Code'] = row['Zip/Postal Code']
            output_row['Home Country'] = row['Country']
            yield output_row



def translate_paypal(csv_filename, start_date, end_date):
    """Do the translation.
    """
    data_set = PayPalTransactions(csv_filename, start_date, end_date)
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

    # make_simple_subset(args[0], opts.start_date, opts.end_date)
    translate_paypal(args[0], opts.start_date, opts.end_date)


if __name__ == '__main__':
    logging.basicConfig(level=logging.DEBUG)
    main()
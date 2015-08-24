"""
Read the raw mining report and produce two new csv files with
these suffixes:
    -ms.csv   : Market Share
    -tgr.csv  : Total Gold Rank

Use the -i option to print out instructions for updating the Tableau quarterly
report for elves.
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
        self.mining_date_start = datetime.strptime(
            date_start, '%Y-%m-%d').date() if date_start else None
        self.mining_date_end = datetime.strptime(
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
            - if self.mining_date_start, try to check it and return False only if out of range, raise if missing date fields
            - put row[c] in try/except KeyError and raise error if checking missing fields
            - keep check for null contents of present field
        """
        try:
            for c in (self.fieldnames_in + ['Mining Date']):
                if not row[c]:
                    log.info('Skipping row with null %r', c)
                    return False
            mining_date = datetime.strptime(row['Mining Date'], '%Y-%m-%d').date()
            if (mining_date < self.mining_date_start or
                    mining_date >= self.mining_date_end):
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
        self.fieldnames_in = ['Elf Name', 'Mining Date', 'Gem Invoice']
        self.fieldnames_out = self.fieldnames_in
        self.new_csv_name = os.path.splitext(self.csv_filename)[0] + '-new.csv'

    def keep_me(self, row):
        """Could be slightly different.
        """
        return super(SimpleSubset, self).keep_me(row)


class TotalGoldRank(GetDataSet):
    """Create a data set that shows the rank by total gold.
    Input file: a csv produced by the Elven Report Bureau.
    Output file name suffix: -tgr

    """
    def __init__(self, csv_filename, date_start, date_end):
        super(TotalGoldRank, self).__init__(csv_filename, date_start,
                                                      date_end)
        self.fieldnames_in = ['Elf Name', 'Gold']
        # ambiguously, the title for the TGR column (for each row) works
        # fine also for the output column, TGR for each elf
        self.fieldnames_out = self.fieldnames_in + ['Rank']
        self.new_csv_name = os.path.splitext(self.csv_filename)[0] + '-tgr.csv'
        self.get_rows = self.rank_tgr_by_elf

    def rank_tgr_by_elf(self):
        """Rank the total gold for each elf.
        """
        totals = dict()
        for row in self.get_csv_bits():
            if row['Elf Name'] in totals.keys():
                totals[row['Elf Name']] += Decimal(row['Gold'])
            else:
                totals[row['Elf Name']] = Decimal(row['Gold'])
        # now we have a dict by Elf Name of the total gold for each
        # calculate the rank of each elf
        rank = sorted(totals.items(), key=lambda t: t[1], reverse=True)
        rank = [rank[i] + (i+1,) for i in range(len(rank))]
        # turn this into a little spreadsheet with three columns
        # yield one elf per row
        for total_row in rank:
            yield dict(zip(self.fieldnames_out, total_row))

"""
        rank = sorted(totals.items(), key=lambda t: t[1], reverse=True)

(Pdb) type(totals)
<type 'dict'>
(Pdb) type(totals.items())
<type 'list'>
(Pdb) type(totals.items()[0])
<type 'tuple'>
(Pdb) type(t)
<type 'tuple'>
(Pdb) type(t[1])
<class 'decimal.Decimal'>

        rank = [rank[i] + (i+1,) for i in range(len(rank))]

(Pdb) type(rank[i])
<type 'tuple'>
(Pdb) type(rank[i] + (i+1,))
<type 'tuple'>

"""

class GemTypeLookup(GetDataSet):
    """Get the simple data set for Gem Type Lookup and use it
    to create the Color and Gem categories.
    """
    GEM_TYPE_LOOKUP_DATA = "colo.csv"
    GEM_TYPE = 'Gem Type'
    GEM_COLOR = 'Color'

    COLOR_TO_COLORCAT = dict([
        ("Cerulean", "Blue"),
        ("Teal", "Blue"),
        ("Cyan", "Blue"),
        ("Azure", "Blue"),
        ("Turquoise", "Blue"),
        ("Green", "Green"),
        ("Purple", "Purple"),
        ("Orange", "Orange"),
        ("Red", "Red"),
        ("Brown", "Brown"),
        ("Yellow", "Yellow"),
        ("Magenta", "Magenta"),
        ("Fuscia", "Magenta"),
    ])

    def __init__(self, csv_filename):
        super(GemTypeLookup, self).__init__(csv_filename)
        # get rid of Color Cat since it comes from the above dict
        self.fieldnames_in = [self.GEM_TYPE, self.GEM_COLOR]
        self.fieldnames_out = self.fieldnames_in

    def keep_me(self, row):
        return True


class MarketShareAnalysis(GetDataSet):
    """Create a data set for the Total Weight by elf by
    Gem Color.
    """
    def __init__(self, csv_filename, date_start, date_end):
        super(MarketShareAnalysis, self).__init__(csv_filename, date_start,
            date_end)
        self.new_csv_name = os.path.splitext(self.csv_filename)[0] + '-ms.csv'
        self.fieldnames_in = ['Elf Name', 'Elf ID', 'Gem Type', 'Weight', 'Quantity']
        self.fieldnames_out = ['Color Cat', 'Gem Color', 'Elf Name', 'Elf ID', 'Total Weight', 'Rank in Gem Color']
        self.gem_rows = self.lookup_gem_rows()
        self.get_rows = self.elf_grams_by_gem_color
        self.elf_ids = None

    def lookup_gem_rows(self):
        """Create a dict keyed by the Gem Type. This is not a get_rows
        iterator inside GemTypeLookup because we need to use this dict
        as a lookup table. We do not need to iterate over the input just once.
        """
        gem_rows = dict()
        gem_colors = GemTypeLookup(GemTypeLookup.GEM_TYPE_LOOKUP_DATA)
        for row in gem_colors.get_csv_bits():
            gem_rows[row[gem_colors.GEM_TYPE]] = row[gem_colors.GEM_COLOR]
        return gem_rows

    def get_gem_rows(self):
        return self.gem_rows

    def add_grams_for_gem_color(self, gem_color_dict, row):
        """Add up the grams by Gem Color.
        """
        gem_color = self.gem_rows[row['Gem Type']]
        total_grams = Decimal(row['Weight']) * Decimal(row['Quantity'])
        # is the Gem Color for this Gem Type in the output yet?
        if gem_color not in gem_color_dict:
            gem_color_dict[gem_color] = total_grams
        else:
            gem_color_dict[gem_color] += total_grams

    def elf_grams_by_gem_color(self):
        """Use the Gem Type Lookup data set and the mining report to
        calculate the Total Weight by elf, Gem Color and Color Cat.
        self.fieldnames_in = ['Elf Name', 'Gem Type', 'Weight', 'Quantity']

        output_per_elf = {
            elfA: {
                gem_color_lookup1 : total_grams,
                gem_color_lookup2 : total_grams,
            }
            elfB: {
                gem_color_lookup1 : total_grams,
                gem_color_lookup2 : total_grams,
            }
        }
        """
        output_per_elf = dict()
        self.elf_ids = dict()
        for row in self.get_csv_bits():
            elf = row['Elf Name']
            self.elf_ids[elf] = row['Elf ID']
            # is this elf in the output yet?
            if elf not in output_per_elf.keys():
                output_per_elf[elf] = dict()
            self.add_grams_for_gem_color(output_per_elf[elf], row)
        # could not fill in the ranking dict until all the rows
        # were added up for each color

        # make a dictionary of gem_color:(tuple of all the fields except rank)
        gem_colors = dict()
        for elf, elf_gem_colors in output_per_elf.iteritems():
            for gem_color, total_grams in elf_gem_colors.iteritems():
                if gem_color in gem_colors:
                    gem_colors[gem_color].append((GemTypeLookup.COLOR_TO_COLORCAT[gem_color],
                       gem_color, elf, self.elf_ids[elf], total_grams))
                else:
                    gem_colors[gem_color] = [(GemTypeLookup.COLOR_TO_COLORCAT[gem_color],
                       gem_color, elf, self.elf_ids[elf], total_grams)]

        for gem_color, elf_grams in gem_colors.iteritems():
            rank = sorted(elf_grams, key=lambda t: t[4], reverse=True)
            rank = [rank[i] + (i+1,) for i in range(len(rank))]
            for row in rank:
                # self.fieldnames_out = ['Color Cat', 'Gem Color', 'Elf Name', 'Elf ID', 'Total Weight', 'Rank in Gem Color']
                yield dict(zip(self.fieldnames_out, row))


class AllColorTotals(MarketShareAnalysis):
    """Create a data set for the Total Weight by
    Gem Color.
    """
    def __init__(self, csv_filename, date_start, date_end):
        super(AllColorTotals, self).__init__(csv_filename, date_start,
                                               date_end)
        self.new_csv_name = os.path.splitext(self.csv_filename)[0] + '-allco.csv'
        self.fieldnames_in = ['Gem Type', 'Weight', 'Quantity']
        self.fieldnames_out = ['Color Cat', 'Gem Color', 'Total Weight']
        self.get_rows = self.all_grams_by_gem_color
        self.totals_by_color = None

    def all_grams_by_gem_color(self):
        """Just like elf_wight_by_gem_color, but instead of calculating the
        Total Weight per elf for each Gem Color, just do it all together.
        """
        totals_by_color = dict()
        for row in self.get_csv_bits():
            self.add_grams_for_gem_color(totals_by_color, row)
        # save this for later lookup by color in python, not Tableau
        self.totals_by_color = totals_by_color
        for gem_color, total_grams in totals_by_color.iteritems():
            row = (GemTypeLookup.COLOR_TO_COLORCAT[gem_color],
                   gem_color, total_grams)
            yield dict(zip(self.fieldnames_out, row))



class MarketShareAnalysisMatrix(GetDataSet):
    """Create a combo dataset for everything needed in the
    MarketShare Analysis report.
    """
    def __init__(self, csv_filename, date_start, date_end):
        super(MarketShareAnalysisMatrix, self).__init__(csv_filename,
                                                          date_start,
                                                          date_end)
        self.fieldnames_out = [
            'Elf Name',
            'Elf ID',
            'Color Cat',
            'Gem Color',
            'Total Weight 2014',
            'Total Weight 2015',
            '2015 vs. 2014',
            '2015 Mining Market Share',
            '2015 Color Rank',
            ]

        # save the dates in their string form to pass to other classes
        self.date_start = date_start
        self.date_end = date_end
        self.new_csv_name = os.path.splitext(self.csv_filename)[0] + '-ms.csv'
        self.get_rows = self.calculate_MarketShare_matrix

    def double_key_elf_color(self, list_of_dicts):
        """Make a double index dict where the key is the tuple (elf,colorcat)
        from the dict that is then the value for that key.
        """
        double_key = dict()
        for row in list_of_dicts:
            double_key[row['Elf Name'], row['Gem Color']] = row
        return double_key

    def calculate_MarketShare_matrix(self):
        """Call each class and save results as a list, then yield the rows for
        the full matrix.

        """
        data_2014 = MarketShareAnalysis(self.csv_filename, '2014-1-1', '2015-1-1')
        elfncolor_2014 = self.double_key_elf_color(data_2014.save_list_of_dicts())
        # ['Color Cat', 'Gem Color', 'Elf Name', 'Total Weight', 'Rank in Gem Color']
        data_2015 = MarketShareAnalysis(self.csv_filename,
                                      self.date_start, self.date_end)
        elfncolor_2015 = self.double_key_elf_color(data_2015.save_list_of_dicts())
        gem_rows = data_2015.get_gem_rows()
        data_2015_all = AllColorTotals(self.csv_filename,
                                         self.date_start, self.date_end)
        data_2015_all.save_list_of_dicts()
        # we are reporting on 2015, so use that for the list of elves
        # better to save the list of elves in the other class than this silliness
        elves = data_2015.elf_ids.keys()
        # gem_rows has the complete set of colors
        colors = list(set(gem_rows.values()))
        import itertools
        elves_and_colors = [element for element in itertools.product(elves, colors)]
        for elfncolor in elves_and_colors:
            elf = elfncolor[0]
            color = elfncolor[1]
            try:
                row = {
                    'Elf Name': elf,
                    'Color Cat': GemTypeLookup.COLOR_TO_COLORCAT[color],
                    'Gem Color': color,
                    'Elf ID': data_2015.elf_ids[elf],
                }
            except KeyError:
                log.info("Skipping unknown color %r", color)
                continue
            try:
                enc_2014 = elfncolor_2014[elfncolor]
            except KeyError:
                enc_2014 = None
            if enc_2014:
                row['Total Weight 2014'] = enc_2014['Total Weight']
            else:
                row['Total Weight 2014'] = None
            try:
                enc_2015 = elfncolor_2015[elfncolor]
            except KeyError:
                enc_2015 = None
            prev_year = None
            if enc_2015:
                row['Total Weight 2015'] = enc_2015['Total Weight']
                row['2015 Color Rank'] = enc_2015['Rank in Gem Color']
                if row['Total Weight 2014']:
                    prev_year = (Decimal(row['Total Weight 2015']) /
                                 Decimal(row['Total Weight 2014']))
            else:
                row['Total Weight 2015'] = None
                row['2015 Color Rank'] = None
            row['2015 vs. 2014'] = prev_year
            # get the total in this Gem Color for 2015 so far, if any
            try:
                total_2015 = data_2015_all.totals_by_color[color]
            except KeyError:
                total_2015 = None
            if total_2015 and row['Total Weight 2015']:
                row['2015 Mining Market Share'] = (row['Total Weight 2015'] / total_2015)
            else:
                row['2015 Mining Market Share'] = None
            yield row


def make_rank_by_tgr(csv_filename, start_date, end_date):
    """Get the rank by total gold dataset.
    """
    data_set = TotalGoldRank(csv_filename, start_date, end_date)
    lines = data_set.write_new_csv()
    print "wrote %r lines to %r" % (lines, data_set.new_csv_name)



def make_market_share_data(csv_filename, start_date, end_date):
    """Put all of Market Share columns into a single csv.
    """
    data_set = MarketShareAnalysisMatrix(csv_filename, start_date, end_date)
    lines = data_set.write_new_csv()
    print "wrote %r lines to %r" % (lines, data_set.new_csv_name)

def show_notes():
    """Show notes about creating data sets to use in Tableau.
    """
    print "TotalGoldRank: ", TotalGoldRank.__doc__
    print "MarketShareAnalysis: ", MarketShareAnalysis.__doc__
    print """

Fixups:
 - fix number formats for 2015 vs. 2014, 2015 Market Share
 - sort by Rank on Top N by Total Gold amount sheet

TotalGoldRank
-------------

Output file name suffix: -tgr

This value is used in Tableau as a fixed rank number to show when the rest of
the data is filtered by elf.  For example, showing one elf with its rank
within the set of all elf.  Without the pre-calculated rank in its own data
source, the rank of one single elf will always be #1!  So this data source is
not needed for the Top Ten (or Top N) elves - the rank calculated on that sheet
will be correct.  It is only needed for the performance view of a single elf,
where the rank should be calculated against all elves, not just the one
selected.

To hide the elf names on the Top Ten worksheet, copy the Elf field in
the original data source, use the copy, make sure the totals and rank are still
correct, and then alias the names to blanks.  To avoid problems with invisible
names, give rank one a single space; rank 2 double; rank 3, 3 spaces; etc.


MarketShareAnalysis
-------------------

Output file name suffix: -ms

Uses MarketShareAnalysisMatix, GemTypeLookup (which requires the hard coded
gem type lookup file colo.csv), AllColorTotals.

Joins and data blends as alternatives to this code have different drawbacks.


    """


def main():
    """Run the script."""
    usage = """usage %prog arg1

    arg1 is the name of the csv file to read.
    """
    # just for easy testing
    DATE_START = '2015-01-01'  # inclusive
    DATE_END = '2015-07-01'    # exclusive

    parser = optparse.OptionParser(usage=usage)
    parser.add_option(
        '-s',
        '--start_date',
        default=DATE_START,
        help='Inclusive date mining start date.',
    )
    parser.add_option(
        '-n',
        '--note',
        action='store_true',
        dest='note',
        help='Show report notes.',
    )
    parser.add_option(
        '-e',
        '--end_date',
        default=DATE_END,
        help='Exclusive date mining end date.',
    )
    (opts, args) = parser.parse_args()
    if opts.note:
        show_notes()
        print parser.format_help()
        exit()

    if not args:
        print parser.format_help()
        exit()
        # raise optparse.BadOptionError('CSV file name required.')

    make_rank_by_tgr(args[0], opts.start_date, opts.end_date)
    make_market_share_data(args[0], opts.start_date, opts.end_date)


if __name__ == '__main__':
    logging.basicConfig(level=logging.DEBUG)
    main()

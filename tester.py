# Eshwar Umarengan and Yug Purohit
# Intermediate Data Programming
# File for testing 

import warnings
warnings.simplefilter(action='ignore', category=FutureWarning)

import cse163_utils as cse
from Stat_Reader import Stat_Reader as s
  
def main():
    '''
    Delegator method that runs various testing methods
    '''
    tester = s()
    test_data_edit(tester)
    test_printer(tester)


def test_data_edit(tester):
    '''
    function to test data_edit method by checking for correct # of cols and rows of edited dataset
    '''
    test_data = tester.load_in_data('year_1.csv')
    # tests if # of rows in edited dataset are correct
    cse.assert_equals(322, len(tester.data_edit(test_data, 'year_1.csv')))
    # tests if # of cols in edited dataset are correct
    cse.assert_equals(17, len(tester.data_edit(test_data, 'year_1.csv').columns))


def test_printer(tester):
    '''
    function to test main datasets created by printing first rows of each one
    '''
    tester.printer()


if __name__ == '__main__':
    main()
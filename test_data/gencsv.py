#!/usr/bin/python3
import pandas as pd
import sys
import argparse as ap 
import string

def get_args():
  parser = ap.ArgumentParser(
    prog='./gencsv.py',
    description='Generates random basic csv')
  parser.add_argument(
    '-t, --types_list',
    metavar='t',
    type=str, 
    nargs='+',
    required=True,
    help='list of data types to appear in the output file',
    dest='types_list')
  parser.add_argument(
    '-n, --num_lines',
    metavar='n',
    type=int, 
    nargs=1,
    required=True,
    help='number of lines to appear in the output file',
    dest='num_lines')
  parser.add_argument(
    '-f, --file_name',
    metavar='f',
    type=str,
    default='simple.csv', 
    nargs=1,
    help='number of lines to appear in the output file',
    dest='file_name')
  parser.add_argument(
    '-s, --str_max_len',
    metavar='s',
    type=int,
    default=[32], 
    nargs=1,
    help='maximum length of str type',
    dest='str_max_len')
  args_raw = parser.parse_args()
  accepted_types = ['str', 'int64', 'float64']
  types = pd.Series(args_raw.types_list)
  if ~types.isin(accepted_types).all():
    bad = types.loc[~types.isin(accepted_types)].str.cat(sep=', ')
    raise ValueError('Unsupported dtypes provided: {}'.format(bad))
  args_raw.num_lines = args_raw.num_lines[0]
  if args_raw.num_lines <= 0:
    raise ValueError('Number of lines must be positive')
  args_raw.str_max_len = args_raw.str_max_len[0]
  if args_raw.str_max_len <= 0:
    raise ValueError('Maximum length of str type must be positive')
  return args_raw

try:
  args = get_args()
except ValueError as verr:
  print(str(verr))
  print("Invalid arguments")
  sys.exit(0)

type_counts = pd.Series(args.types_list).value_counts()
char_arr = pd.np.array(list(string.digits + string.ascii_letters))
num_strs = type_counts.loc['str']
char_arr_size = [args.num_lines, num_strs, args.str_max_len]
strs = pd.DataFrame(pd.np.apply_along_axis(
  lambda y: pd.np.str.join('', y), 
  axis=2, 
  arr=pd.np.random.choice(char_arr, char_arr_size)))

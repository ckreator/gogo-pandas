#!/usr/bin/python3
import sys
import string
import pandas as pd
import argparse as ap
from functools import partial
from timeit import timeit

def main():
  acc_types = ['str', 'int64', 'float64']
  try:
    args = get_args(acc_types)
    funcs = {
      'str': partial(get_strs, nrow=args.num_lines, lmax=args.str_max_len),
      'int64': partial(get_int64s, nrow=args.num_lines),
      'float64': partial(get_float64s, nrow=args.num_lines)
    }
    type_counts = pd.Series(args.types_list).value_counts()
    out = pd.DataFrame(pd.np.zeros((args.num_lines, len(args.types_list))))
    cols = pd.np.array(args.types_list)
    for type_str in type_counts.index.tolist():
      col_select = cols == type_str
      frm = funcs[type_str](type_counts.loc[type_str]).astype(object)
      out.loc[:, col_select] = frm.values
    out.to_csv(args.file_name, header=False, index=False)
    test(args.file_name)
  except ValueError as verr:
    print(str(verr))
    print("Invalid arguments")
    sys.exit(0)

def get_args(accepted_types):
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
    default='outfile.csv', 
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

def get_strs(ncol, nrow, lmax):
  char_arr = pd.np.array(list(string.digits + string.ascii_letters))
  return pd.DataFrame(pd.np.apply_along_axis(
            lambda y: pd.np.str.join('', y), 
            axis=2, 
            arr=pd.np.random.choice(char_arr, (nrow, ncol, lmax))))

def get_int64s(ncol, nrow):
  return pd.DataFrame(pd.np.random.randint(
          pd.np.iinfo(pd.np.int64).min,
          pd.np.iinfo(pd.np.int64).max,
          size=(nrow, ncol),
          dtype=pd.np.int64))

def get_float64s(ncol, nrow):
  return pd.DataFrame(pd.np.random.rand(nrow, ncol))

def test(file_name):
  def test_fn():
    return pd.read_csv(file_name, engine='c')
  n_rep = 1000
  time_sec = timeit(test_fn, number=n_rep)
  print('Pandas will parse the file {} times in: {}s'.format(n_rep, time_sec))

if __name__ == "__main__":
  main()

# -*- coding: utf-8 -*-
from os import listdir,chdir
from os.path import isfile
import pandas as pd
import numpy as np
import argparse

"""
Script for merging keyword research from multiple sources
"""

# The folder the csv documents are placed in
csv_folder = '<folder containing your keyword files>'

# Configuration settings for importing into RankFalcon
site = 'http://www.rankfalcon.com'
search_engine = 'google.com'
language = 'English'

# Input/output filename
search_console_input_file = 'www-rankfalcon-com_20160917T193416Z_SearchAnalytics.csv'
manual_keywords_input_file = 'manual_keywords.csv'
output_file_name = 'keyword_research.csv'
output_file_name_rankfalcon = 'rankfalcon_csv_import.csv'

output_columns = [
    'Keyword',
    'Search volume',
    'CPC',
    'Competition',
    'Clicks',
    'Impressions',
    'CTR',
    'Position',
    'Quality',
    'Potential'
]

# Filter settings
cutoff = 250  # How many keywords you want

def merge_files():
    """
    @return: list of keywords with their data
    @return type: list of dictionaries
    """
    # Search console
    search_console_frame = pd.read_csv(search_console_input_file, index_col=0)
    search_console_frame.columns = ['impressions', 'clicks', 'CTR', 'position']
    search_console_frame.index = search_console_frame.index.str.replace('+', ' ')

    # Keyword planner
    frame_list = []
    keyword_planner_files = [f for f in listdir('.') if isfile(f) and f.startswith('Keyword Planner')]
    for keyword_planner_file in keyword_planner_files:
        keyword_planner_frame = pd.read_csv(keyword_planner_file, delimiter='\t', usecols=['Keyword', 'Avg. Monthly Searches (exact match only)', 'Competition', 'Suggested bid'], header=0, encoding='utf-16', index_col=0)
        keyword_planner_frame.columns = ['search volume', 'competition', 'CPC']
        keyword_planner_frame['search volume'] = keyword_planner_frame['search volume'].str.replace(u'–','-')  # Reformat en dash to normal dash
        frame_list.append(keyword_planner_frame)
    keyword_planner_frame = pd.concat(frame_list, axis=0)
    keyword_planner_frame.drop_duplicates(inplace=True)
    df = pd.merge(search_console_frame, keyword_planner_frame, left_index=True, right_index=True, how='outer')

    # Manual keywords
    manual_keywords_frame = pd.read_csv(manual_keywords_input_file, index_col=0)
    df = pd.merge(df, manual_keywords_frame, left_index=True, right_index=True, how='outer')
    df = df[['search volume', 'CPC', 'competition', 'impressions', 'clicks', 'CTR', 'position']]
    df.index.name = 'keyword'

    # Deleting duplicates from the index
    df = df.reset_index().drop_duplicates(subset='keyword', keep='last').set_index('keyword')
    return df

def output_data_csv(df):
    df.to_csv(output_file_name, encoding='utf-8')

def read_merged_file(output_file_name):
    """This function is used when updating data manually
    It reads the output file"""
    df = pd.read_csv(output_file_name, index_col=0, header=0)
    return df

def rankfalcon_csv_import(df):
    fieldnames = [
        'site',
        'search_engine',
        'location_name',
        'groups',
        'language'
    ]
    df_cut_off = df[:cutoff]
    df_cut_off['site'] = site
    df_cut_off['search_engine'] = search_engine
    df_cut_off['language'] = language
    df_cut_off['location_name'] = np.nan
    df_cut_off['groups'] = np.nan
    df_cut_off = df_cut_off[fieldnames]
    df_cut_off.to_csv(output_file_name_rankfalcon, encoding='utf-8')
 
def sort_keywords(df):
    return df.sort_values(by=['potential'], ascending=False)

def keyword_quality(df):
    """
    An estimate of how good quality a keyword is for your site
    A high CPC shows this keyword is a high quality keyword in general
    A high CTR shows this keyword is relevant to your site (0 is used if data not available)
    @param keyword:
    @return:
    """
    df_sub = df.fillna({'CTR': 0, 'CPC': 0})
    df_sub['CTR'] = df_sub['CTR'].replace('%', '', regex=True).astype('float')
    df['quality'] = 5 * df_sub['CTR'] + df_sub['CPC']
    return df

def keyword_potential(df):
    """
    A indicator for selecting keyword to work on
    Takes into account:
    keyword quality: favours high quality keywords
    position: more potential to improve on a bad ranking
    competition: easier to improve on less competitive keywords
    search volume: Favours mid range keywords (i.e. not too specific and not too generic)
    @param keyword:
    @return:
    """
    def search_volume_map(search_volume):
        map = {
            1.0: 1.0,
            '1 - 10': 0.5,
            '10 - 100': 1.0,
            '100 - 1K': 3.0,
            '1K - 10K': 3.0,
            '10K - 100K': 2.0,
            '100K - 1M': 1.0,
            '1M - 10M': 0.5,
        }
        return map[search_volume]
    df_sub = df.fillna({'position': 10, 'competition': 1, 'search volume': 1.0})
    df['potential'] = df_sub['quality'] * 0.01 * df_sub['position'] * df_sub['search volume'].apply(search_volume_map) / (df_sub['competition']+1e-4)
    return df

def calculate_metrics(df):
    df = keyword_quality(df)
    df = keyword_potential(df)
    df = sort_keywords(df)
    return df

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='process keywords')
    parser.add_argument("--process", help='merges all your keyword files', action="store_true")
    parser.add_argument("--update", help='for recalculating qualilty and potential if you manually update some data', action="store_true")
    parser.add_argument("--rankfalcon", help='for creating a file of all your keyword ready for importing into RankFalcon keyword tracking tracking tool', action="store_true")
    chdir(csv_folder)
    args = parser.parse_args()
    if args.process:
        df = merge_files()
        df = calculate_metrics(df)
        output_data_csv(df)
        print('Keyword files processed. ' + output_file_name + ' has been created.')
    if args.update:
        df = read_merged_file(output_file_name)
        df = calculate_metrics(df)
        output_data_csv(df)
        print(output_file_name + ' has been updated.')
    if args.rankfalcon:
        try:  # Check first processing step has been done
            df = read_merged_file(output_file_name)
            rankfalcon_csv_import(df)
            print(output_file_name_rankfalcon + ' has been created. You are now ready to import your keywords into rankfalcon.com keyword tracking tool.')
        except Exception:  # If not create intermediate file too
            df = merge_files()
            df = calculate_metrics(df)
            output_data_csv(df)
            print('Keyword files processed. ' + output_file_name + ' has been created.')
            rankfalcon_csv_import(df)
            print(
            output_file_name_rankfalcon + ' has been created. You are now ready to import your keywords into rankfalcon.com keyword tracking tool.')
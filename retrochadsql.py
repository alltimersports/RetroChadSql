#!/usr/bin/python2


"""RetroChadSql uses Tk and takes no command arguments. It has three
public constants, in case someone chooses to inspect it from elsewhere:

LICENSE is the terms under which RetroChadSQL is liceensed.
VERSION is the RetroChadSQL version number.
AVAILABLE_YEARS is a string indicating the years Retrosheet is known to
provide.

Required: Python 2.7 (or possibly Python 3 and automatic conversion) and
the Tk librarires that are usually but not always installed with Python.

RetroChadSql can perform all or some of its tasks, which it will do for
each year the user selects. Those tasks are:
-- downloading zipped play by play data from Retrosheet
-- unzipping those files
-- using Chadwick to assemble the data into CSV files
-- writing SQL data definitions, using the structure of the data
-- loading the data into an SQL database

Licensing information may be read in the code directly following the
import statements and is visible in the Tk window.

"""
"""RetroChadSql is packaged as a single file for easiest downloading and
execution. If it is to persist and be maintained in a particular
installation, it would make sense to convert it to a package. Notes
relating to that possible conversion are included in comments within
the file.

"""


import Tkinter as tk
import ttk
import tkFont
import tkFileDialog as tkf
from tkMessageBox import showwarning
from ScrolledText import ScrolledText
import os
import platform
import webbrowser
import collections
import sys
import urllib2
from BaseHTTPServer import BaseHTTPRequestHandler as BHRH
from contextlib import closing
from zipfile import ZipFile, BadZipfile
import subprocess
import re


LICENSE = """Copyright (c) 2014, All Timer Sports and Dvd Avins
All rights reserved.

Redistribution and use in source and binary forms, with or without
modification, are permitted provided that the following conditions are met:
    * Redistributions of source code must retain the above copyright
      notice, this list of conditions and the following disclaimer.
    * Redistributions in binary form must reproduce the above copyright
      notice, this list of conditions and the following disclaimer in the
      documentation and/or other materials provided with the distribution.
    * Neither the name of All Timer Sports nor the
      names of its contributors may be used to endorse or promote products
      derived from this software without specific prior written permission.

THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND
ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDERS BE LIABLE FOR ANY
DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES
(INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND
ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
(INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE."""

VERSION = '0.9.0'

AVAILABLE_YEARS = '1921 1922 1927 1931 1938-2012'
"""The years Retrosheet has play-by-play records available for
downloading.

AVAILABLE_YEARS ::= spec [' 'spec]*
spec ::= year | range
year ::= 'nnnn'
range ::= 'nnnn-mmmm' where mmmm > nnnn; range is inclusive.

"""


class Environment(object):
    """Information about and methods for investigating the user's
    environment.

    __init()__ populates the atributes.
    system is the result of platform.system(). OSX returns 'Darwin.'
    user_dir is the user's home directory.
    sep is user's file system's path seperator.

    """

    def __init__(self):
        self.system = platform.system()
        self.user_dir = os.path.expanduser('~')
        self.line_sep = os.linesep

    def exist_path(self, path):
        # Return the deepest existing path-part of a path, else None.
        if os.path.exists(path):
            return path
        up = os.path.dirname(path) # Up one level, if possible.
        if up == path:  # Root of a mising drive or malformed.
            return None
        return self.exist_path(up)


    def get_sql_client(self):        
        # Find likely SQL command shell. Return a tuple: (
        # boolean indicating a likely SQL client is in system PATH,
        # file_path of that likely client, even if it's not in PATH).
        # If no candidate is found, returns (False, None).
        
        #sql_shells = ['psql', 'mysql']  Only suppport MySQL now.
        sql_shells = ['mysql']
        path_env = os.environ['PATH']
        if self.system == 'Windows':
            sql_shells = [shell + '.exe' for shell in sql_shells]
            path_dirs = path_env.split(';')
        else:
            path_dirs = path_env.split(':')
        for path_dir in path_dirs:
            try:
                dir_list = os.listdir(path_dir)
            except OSError:
                continue
            for sql_shell in sql_shells:
                if sql_shell in dir_list:
                    return (True, os.path.join(path_dir, sql_shell))
        
        if self.system == 'Windows':
            # Finds 64-bit programs before 32-bit.
            sys_prog_dirs = ['C:\\Program Files', 'C:\\Program Files (x86)']
            #shell_dict = dict(zip(['PostgreSQL', 'MySQL'], sql_shells))
            shell_dict = dict(zip(['MySQL'], sql_shells))
            for sys_prog_dir in sys_prog_dirs:
                try:
                    # Reverse to find 'p' before 'm'.
                    prog_names = reversed(os.listdir(sys_prog_dir))
                except OSError:
                    continue
                for prog_name in prog_names:
                    try:
                        shell_name = shell_dict[prog_name]
                    except KeyError:
                        continue
                    prog_dir = os.path.join(sys_prog_dir, prog_name)
                    # Reverse to find later versions first.
                    for sub_name in reversed(os.listdir(prog_dir)):
                        shell_path = os.path.join(
                            prog_dir, sub_name, 'bin', shell_name)
                        if os.path.exists(shell_path):
                            return (False, shell_path)
        else:
            #shell_dirs = dict(zip(sql_shells, ['pgsql','mysql']))
            shell_dirs = dict(zip(sql_shells, ['mysql']))
            for sql_shell in sql_shells:
                for try_path in (
                        os.path.join(['/usr/bin/', sql_shell]),
                        os.path.join(['/usr/local', shell_dirs[sql_shell],
                                      'bin', sql_shell])):
                    if os.path.exists(try_path):
                        return (False, try_path)
                    
        return (False, None)


class Table:

    _sql_data_types = {'count': {'MySQL': 'MEDIUMINT UNSIGNED'},
                       'date': {'MySQL': 'DATE'},
                       'datetime': {'MySQL': 'DATETIME'},
                       'flag': {'MySQL': 'TINYINT UNSIGNED'},
                       'text': {'MySQL': 'VARCHAR(200)'},
                       'time': {'MySQL': 'TIME'}}

    _column_types_literal = {  # {data_type: {table: [columns]}}
        'count': {
            'events': [
                'INN_CT', 'OUTS_CT', 'BALLS_CT', 'STRIKES_CT', 'AWAY_SCORE_CT',
                'HOME_SCORE_CT', 'BAT_FLD_CD', 'BAT_LINEUP_ID', 'EVENT_CD',
                'H_CD', 'EVENT_OUTS_CT', 'RBI_CT', 'FLD_CD', 'ERR_CT',
                'ERR1_FLD_CD', 'ERR2_FLD_CD', 'ERR3_FLD_CD', 'BAT_DEST_ID',
                'RUN1_DEST_ID', 'RUN2_DEST_ID', 'RUN3_DEST_ID',
                'REMOVED_FOR_PH_PAT_FLD_CD', 'PO1_FLD_CD', 'PO2_FLD_CD',
                'PO3_FLD_CD', 'ASS1_FLD_CD', 'ASS2_FLD_CD', 'ASS3_FLD_CD',
                'ASS4_FLD_CD', 'ASS5_FLD_CD', 'EVENT_ID', 'START_FLD_SCORE',
                'INN_RUNS_CT', 'GAME_PA_CT', 'INN_PA_CT', 'START_BASES_CD',
                'END_BASES_CD', 'RUN1_FLD_CODE', 'RUN1_LINEUP_CD',
                'RUN1_ORIGIN_EVENT_ID', 'RUN2_FLD_CODE', 'RUN2_LINEUP_CD',
                'RUN2_ORIGIN_EVENT_ID', 'RUN3_FLD_CODE', 'RUN3_LINEUP_CD',
                'RUN3_ORIGIN_EVENT_ID', 'PA_BALL_CT', 'PA_CALLED_BALL_CT',
                'PA_INTENT_BALL_CT', 'PA_PITCHOUT_BALL_CT',
                'PA_HITBATTER_BALL_CT', 'PA_OTHER_BALL_CT', 'PA_STRIKE_CT',
                'PA_CALLED_STRIKE_CT', 'PA_SWINGMISS_STRIKE_CT',
                'PA_FOUL_STRIKE_CT', 'PA_INPLAY_STRIKE_CT',
                'PA_OTHER_STRIKE_CT', 'EVENT_RUNS_CT', 'BAT_FATE_ID',
                'RUN1_FATE_ID', 'RUN2_FATE_ID', 'RUN3_FATE_ID', 'FATE_RUNS_CT',
                'ASS6_FLD_CD', 'ASS7_FLD_CD', 'ASS8_FLD_CD', 'ASS9_FLD_CD',
                'ASS10_FLD_CD'],
            'games': [
                'GAME_CT', 'START_GAME_TM', 'ATTEND_PARK_CT',
                'METHOD_RECORD_CD', 'PTICHES_RECORD_CD', 'TEMP_PARK_CT',
                'WIND_DIRECTION_PARK_CD', 'WIND_SPEED_PARK_CT','FIELD_PARK_CD',
                'PRECIP_PARK_CD', 'SKY_PARK_CD', 'MINUTES_GAME_CT', 'INN_CT',
                'AWAY_SCORE_CT', 'HOME_SCORE_CT', 'AWAY_HITS_CT',
                'HOME_HITS_CT', 'AWAY_ERR_CT', 'HOME_ERR_CT', 'AWAY_LOB_CT',
                'HOME_LOB_CT', 'AWAY_LINEUP1_FLD_CD', 'AWAY_LINEUP2_FLD_CD',
                'AWAY_LINEUP3_FLD_CD', 'AWAY_LINEUP4_FLD_CD',
                'AWAY_LINEUP5_FLD_CD', 'AWAY_LINEUP6_FLD_CD',
                'AWAY_LINEUP7_FLD_CD', 'AWAY_LINEUP8_FLD_CD',
                'AWAY_LINEUP9_FLD_CD', 'HOME_LINEUP1_FLD_CD',
                'HOME_LINEUP2_FLD_CD', 'HOME_LINEUP3_FLD_CD',
                'HOME_LINEUP4_FLD_CD', 'HOME_LINEUP5_FLD_CD',
                'HOME_LINEUP6_FLD_CD', 'HOME_LINEUP7_FLD_CD',
                'HOME_LINEUP8_FLD_CD', 'HOME_LINEUP9_FLD_CD',
                'AWAY_TEAM_GAME_CT', 'HOME_TEAM_GAME_CT', 'OUTS_CT',
                'AWAY_AB_CT', 'AWAY_2B_CT', 'AWAY_3B_CT', 'AWAY_HR_CT',
                'AWAY_BI_CT', 'AWAY_SH_CT', 'AWAY_SF_CT', 'AWAY_HP_CT',
                'AWAY_BB_CT', 'AWAY_IBB_CT', 'AWAY_SO_CT', 'AWAY_SC_CT',
                'AWAY_CS_CT', 'AWAY_GDP_CT', 'AWAY_XI_CT', 'AWAY_PITCHER_CT',
                'AWAY_ER_CT', 'AWAY_TER_CT', 'AWAY_WP_CT', 'AWAY_BK_CT',
                'AWAY_PO_CT', 'AWAY_A_CT', 'AWAY_PB_CT', 'AWAY_DP_CT',
                'AWAY_TP_CT', 'HOME_AB_CT', 'HOME_2B_CT', 'HOME_3B_CT',
                'HOME_HR_CT', 'HOME_BI_CT', 'HOME_SH_CT', 'HOME_SF_CT',
                'HOME_HP_CT', 'HOME_BB_CT', 'HOME_IBB_CT', 'HOME_SO_CT',
                'HOME_SC_CT', 'HOME_CS_CT', 'HOME_GDP_CT', 'HOME_XI_CT',
                'HOME_PITCHER_CT', 'HOME_ER_CT', 'HOME_TER_CT', 'HOME_WP_CT',
                'HOME_BK_CT', 'HOME_PO_CT', 'HOME_A_CT', 'HOME_PB_CT',
                'HOME_DP_CT', 'HOME_TP_CT'],
             'subs': [
                 'INN_CT', 'SUB_LINEUP_ID', 'SUB_FLD_CD', 'REMOVED_FLD_CD',
                 'EVENT_ID']},
        'date': {
            'games': ['GAME_DT']},
        'datetime': {
            'games': ['INPUT_RECORD_TS', 'EDIT_RECORD_TS']},
        'flag': {
            'events': [
                'BAT_HOME_ID', 'BAT_LAST_ID', 'LEADOFF_FL', 'PH_FL',
                'BAT_EVENT_FL', 'AB_FL', 'SH_FL', 'SF_FL', 'DP_FL', 'TP_FL',
                'WP_FL', 'PB_FL', 'BUNT_FL', 'FOUL_FL', 'RUN1_SB_FL',
                'RUN2_SB_FL', 'RUN3_SB_FL', 'RUN1_CS_FL', 'RUN2_CS_FL',
                'RUN3_CS_FL', 'RUN1_PK_FL', 'RUN2_PK_FL', 'RUN3_PK_FL',
                'GAME_NEW_FL', 'GAME_END_FL', 'PR_RUN1_FL', 'PR_RUN2_FL',
                'PR_RUN3_FL', 'INN_NEW_FL', 'INN_END_FL', 'PA_NEW_FL',
                'PA_TRUNC_FL', 'BAT_START_FL', 'RESP_BAT_START_FL',
                'PIT_START_FL', 'RESP_PIT_START_FL', 'BASE2_FORCE_FL',
                'BASE3_FORCE_FL', 'BASE4_FORCE_FL', 'BAT_SAFE_ERR_FL',
                'UNKNOWN_OUT_EXC_FL', 'UNCERTAIN_PLAY_EXC_FL'],
            'games': ['DH_FL'],
            'subs': ['BAT_HOME_ID']}}

    _field_tweaks = {  # {tweak: (formula, {table: [fields]})}
        'AM_PM': (
            'IF({temp}, STR_TO_DATE({temp}, "%Y/%m/%d %h:%i%p"), NULL)',
            {'games': ['INPUT_RECORD_TS', 'EDIT_RECORD_TS']}),
        'blank_null': (
            'NULLIF({temp}, "")',
            {'games': ['AWAY_TEAM_GAME_CT', 'HOME_TEAM_GAME_CT']}),
        'T_F': (
            'CASE {temp} WHEN "T" THEN TRUE WHEN "F" THEN FALSE END',
            {'events': [
                'LEADOFF_FL', 'PH_FL', 'BAT_EVENT_FL', 'AB_FL', 'SH_FL',
                'SF_FL', 'DP_FL', 'TP_FL', 'WP_FL', 'PB_FL', 'BUNT_FL',
                'FOUL_FL', 'RUN1_SB_FL', 'RUN2_SB_FL', 'RUN3_SB_FL',
                'RUN1_CS_FL', 'RUN2_CS_FL', 'RUN3_CS_FL', 'RUN1_PK_FL',
                'RUN2_PK_FL', 'RUN3_PK_FL', 'GAME_NEW_FL', 'GAME_END_FL',
                'PR_RUN1_FL', 'PR_RUN2_FL', 'PR_RUN3_FL', 'INN_NEW_FL',
                'INN_END_FL', 'PA_NEW_FL', 'PA_TRUNC_FL', 'BAT_START_FL',
                'RESP_BAT_START_FL', 'PIT_START_FL', 'RESP_PIT_START_FL',
                'BASE2_FORCE_FL', 'BASE3_FORCE_FL', 'BASE4_FORCE_FL',
                'BAT_SAFE_ERR_FL', 'UNKNOWN_OUT_EXC_FL',
                'UNCERTAIN_PLAY_EXC_FL'],
             'games': ['DH_FL']}),
        'START_GAME_TM': (
            ('CASE WHEN {temp} = 0 THEN NULL\n      '
             'WHEN DAYNIGHT_PARK_CD = "D" AND {temp} > 800 '
             'THEN {temp} * 100\n      '
             'ELSE ({temp} + 1200) * 100 END'),
            {'games': ['START_GAME_TM']}),
        'WIND_SPEED_PARK_CT': (
            'NULLIF({temp}, -1)',
            {'games': ['WIND_SPEED_PARK_CT']}),
        'year_ct': (
            'SUBSTRING(GAME_ID FROM 4 FOR 4)',
            {'games': ['year_ct'],
             'events': ['year_ct'],
             'subs': ['year_ct']}),
        'zero_null': (
            'NULLIF({temp}, 0)',
            {'games': ['START_GAME_TM']})}
            
    @classmethod
    def set_class_attributes(cls, paths):
        cls._paths = paths.copy()

    def __init__(self, name, envir):
        self._name = name
        self._envir = envir
        self._field_counts = {}

    def _chadwick_command(self, part_keys, year=None):
        # Create a command string for Chadwick.
        command_parts = {'program': '"{chad_path}cw{tool}"',
                         'switches': '-q -n -f 0-{standard_max} -y {year}',
                         'extended': '-x 0-{extended_max}',
                         'arg': '{year}*.EV*',
                         'redirect': ' > "{csv_path}{year} {tool}.csv"',
                         'for_names': '-i 0',
                         'for_description': '-d'}
        assembled = ' '.join([command_parts[key] for key in part_keys])
        dic = {'tool': self._name[:-1],
               'chad_path': self._paths['Chadwick'],
               'csv_path': self._paths['Assemble'],
               'year': year,
               'standard_max': self._field_counts.get('standard', None),
               'extended_max': self._field_counts.get('extended', None)}
        return assembled.format(**dic)

    def parse_description(self):
        # Use Chadwick to make a table's column description dictionary.
        # Also, store the number of standard and extended columns.
        command = self._chadwick_command(['program', 'for_description'])
        description = subprocess.check_output(command, shell=True,
                                              stderr=subprocess.STDOUT)
        reg_exp = r'^(\d+)\s+(.+[^*])\*?$'
        field_index = re.finditer(reg_exp, description, re.MULTILINE)
        
        self._field_comments = []
        field_block = 'standard'
        for field in field_index:
            index = field.group(1)
            if index == '0' and self._field_counts:
                field_block = 'extended'
            self._field_counts[field_block] = index
            comment = field.group(2).rstrip()
            self._field_comments += [comment]
            
    def assemble_year(self, year):
        command_parts = ['program', 'switches', 'arg', 'redirect']
        if 'extended' in self._field_counts:
            command_parts.insert(2, 'extended')
        command = self._chadwick_command(command_parts, year)
        subprocess.call(command, shell=True)

    def _set_field_names(self, year):
        command_parts = ['program', 'switches', 'for_names', 'arg']
        if 'extended' in self._field_counts:
            command_parts.insert(2, 'extended')
        command = self._chadwick_command(command_parts, year)
        header_info = subprocess.check_output(command, shell=True)[:-2]
        self._field_names = [quoted[1:-1] for quoted in header_info.split(',')]

    def _set_column_types(self):
        self._column_types = {}
        for column_type, table_columns in self._column_types_literal.items():
            try:
                columns = table_columns[self._name]
            except KeyError:
                continue
            for column in columns:
                self._column_types[column] = column_type
        self._tweaked_fields = {}
        for tweak, v in self._field_tweaks.items():
            try:
                columns = v[1][self._name]
            except KeyError:
                continue
            for column in columns:
                self._tweaked_fields[column] = tweak

    def define_schema(self, schema, year):
        form = 'CREATE TABLE IF NOT EXISTS {name} (\n  '
        schema.write(form.format(name=self._name))
        
        self._set_column_types()
        self._set_field_names(year)
        column_specs = [
            ('id INT UNSIGNED AUTO_INCREMENT PRIMARY KEY COMMENT '
             '"auto-increment primary key"')]
        form = '{name} {sql_data_type} COMMENT "{comment}"'
        #
        client = 'MySQL' #This should not be hard-coded.
        #
        for name, comment in zip(self._field_names, self._field_comments):
            column_type = self._column_types.get(name, 'text')
            sql_data_type = self._sql_data_types[column_type][client]
            sql_name = name.lower()
            column_specs.append(form.format(
                name=sql_name, sql_data_type=sql_data_type, comment=comment))
        count_type = self._sql_data_types['count'][client]
        column_specs.append(form.format(
            name='year_ct', sql_data_type=count_type, comment='year'))
        schema.write(',\n  '.join(column_specs) + ');\n')
        documentation_form = ('/*\n'
                             'The following form will be used to load data.\n'
                             '{load_form}\n'
                             '*/\n\n\n')
        self._load_form = self._set_load_form()
        schema.write(documentation_form.format(load_form=self._load_form))

    def _set_load_form(self):
        load_form = ('LOAD DATA LOCAL INFILE "{unix_style_path}"\n'
                     '  INTO TABLE {table_name}\n'
                     '  FIELDS TERMINATED BY ","\n'
                     r'    ENCLOSED BY "\""' '\n'
                     '  LINES TERMINATED BY "{line_sep}"\n'
                     '  IGNORE 1 LINES\n'
                     '  ({column_names})\n'
                     '  SET\n'
                     '  {assign_str};')
        file_form = '{{year}} {table_name}.csv'
        file_name = file_form.format(table_name=self._name[:-1])
        os_file_path = os.path.join(self._paths['Assemble'], file_name)
        unix_style_path = os_file_path.replace('\\', '/')
        table_name = self._name
        line_sep = self._envir.line_sep.encode('string-escape')
        effective_names = [
            ('@temp_' if name in self._tweaked_fields else '') + name.lower()
            for name in self._field_names]
        column_str = ', '.join(effective_names)
        assignments = []
        set_form = '  {name} = {formula}'
        for column, tweak in self._tweaked_fields.items():
            formula_form = self._field_tweaks[tweak][0]
            formula = formula_form.format(temp='@temp_' + column.lower())
            assignments.append(set_form.format(
                name=column.lower(), formula=formula))
        assign_str = (',\n  ').join(assignments)
        return load_form.format(
            unix_style_path=unix_style_path, table_name=table_name,
            line_sep=line_sep, column_names=column_str, assign_str=assign_str)
                                
    def load_specs(self, year):
        return self._load_form.format(year=year)


class Tasks(collections.OrderedDict):
    def __init__(self, *args, **kwargs): 
        super(Tasks, self).__init__(*args, **kwargs)
        self.chad_info = {}

    def set_attr(self, attr, val_list, chad=None):
        # attr must be hashable. len(val_list) is expected to be 4 or 5.
        names = (name for name in self.keys())
        for val in val_list:
            name = names.next()
            self[name][attr] = val
        if chad is not None:
            self.chad_info[attr] = chad

    def attr(self, attr):
        attr_dict = {key: self[key][attr] for key in self.keys()}
        try:
            attr_dict['Chadwick'] = self.chad_info[attr]
        except KeyError: pass
        return attr_dict


class Input(object):
    
    """Present the configuration UI to the user; call process(self._config).

    __init__() creates a Tk root for user configuration input and also
    sets some non-public values.
    ask() populates and presents the UI and stores the data in config.    

    """

    def __init__(self, root, constants, envir, tasks, next_func):

        self._root = root
        self._constants = constants
        self._envir = envir
        self._tasks = tasks
        self._when_done = next_func

        #
        """Create, a url style and store some verbiage."""
        
        self._window = ttk.Frame(self._root)
        self._window.grid()
        s = ttk.Style(self._root)
        s.configure('Url.TLabel', foreground='blue')
        self._url_font = tkFont.Font(font=s.lookup('TLabel', 'font')).copy()
        self._url_font.config(underline=1)
        s.configure('Url.TLabel', font=self._url_font)

        self._tasks.set_attr(
            'file_description',
            ['zipped Retrosheet', 'unzipped Retrosheet', 'CSV',
             'SQL definition'],
            chad='Chadwick')
        self._wrap_length = {'Windows': 500, 'Darwin': 750}.get(
            self._envir.system, 600)
        
    def _finish_frame(self, frame, sep=True):
        # Grid frame below parent's lastRow.
        # Then, if sep, grid a vertical bar in parent below the frame.
        #
        # If this file is made into a package, this class should get
        # its own file and this method should be a stand-alone
        # function in that file."""
        parent = frame.master
        try:
            last_row = parent.last_row
        except AttributeError:
            last_row = -10
        frame.grid(row=last_row + 10, sticky='ew')
        if sep:
            sep = ttk.Separator(parent, orient='horizontal')
            sep.grid(row=last_row + 20, columnspan=9999, sticky='ew', pady=3)
            last_row += 20
        else:
            last_row += 10
        parent.last_row = last_row

    def _choose(self, name, var, default, folder, custom_var):
        # Return a function for a button to set var = a file or folder.
        def f():
            init = (self._envir.exist_path(var.get()) or
                    self._envir.exist_path(default) or
                    self._envir._user_dir)
            func, param = ((tkf.askdirectory, 'initialdir') if
                               folder else
                           (tkf.askopenfilename, 'initialfile') if
                               os.path.isfile(init) else
                           (tkf.askopenfilename, 'initialdir'))
            chosen = func(**{'title': name, param: init})                
            if chosen:
                var.set(os.path.abspath(chosen))  # abspath fixes / vs. \.
                try:
                    custom_var.set(True)
                except AttributeError: pass
        return f

    def _get_all(self, var_dict):
        # Return a dict of Python values rather than Tk variables.
        #
        # If this file is made into a package, this class should get
        # its own file and this method should be a stand-alone
        # function in that file.
        return {k: (v.get() if isinstance(v, tk.Variable) else
                    self._get_all(v) if isinstance(v, dict) else
                    v)
                for k, v in var_dict.items()}

    def _choose_var(self, parent, text, default, folder, custom_var=None):
        # Grid a 1-row frame with label, entry for file or folder, and...
        # button to choose that file or folder. Return the var used by...
        # the entry and button.
        frame = ttk.Frame(parent)
        ttk.Label(frame, text=text).grid(padx=3)
        var = tk.StringVar(value=default)
        entry = ttk.Entry(frame, textvariable=var)
        entry.grid(row=0, column=10, sticky='ew', padx=3)
        func = self._choose(text, var, default, folder, custom_var)
        button = ttk.Button(frame, text='Browse...', command=func)
        button.grid(row=0, column=20, padx=3)
        frame.columnconfigure(10, weight=1)
        self._finish_frame(frame, False)
        return var

    def _tk_url(self, master=None, url='', **options):
        # Return a label in the Label.Url style defined in __init__.
        options.setdefault('text', url)
        options['style'] = 'Url.TLabel'
        options.setdefault('cursor', 'hand2')
        label = ttk.Label(master, **options)
        label.bind('<1>', lambda e: webbrowser.open(url))
        return label
        
    def _show_intro(self, nb):
        # Make the tab of introductory help.
        frame = ttk.Frame(nb)
        label = ttk.Label(frame, wraplength=self._wrap_length, text=(
            'Welcome to RetroChadSql, a tool to download Retrosheet event '
            'files and load the data contained in those files into a '
            'relational database.  To use RetroChadSql to its fullest, you '
            'will need...\n'
            '-- an Internet connection\n'
            '-- a relational database\n'
            '-- Chadwick (see the Chadwick tab)\n'
            '-- Python 2.7 with Tkinter (which you have or you wouldn\'t be '
            'seeing this)\n\n'
            'For most users, most of the default settings will be fine. But '
            'you\'ll probably need to...\n'
            '-- set something on the Load tab\n'
            '-- download and/or locate Chadwick\n'
            '-- perhaps customize the years on the General tab\n\n'
            'If you click Pause when RetroChadSql is running, RetroChadSql '
            'will pause when it gets to the end of the task its working on '
            'for the year it\'s working on. Depending on the task, that might '
            'take some time. You won\'t notice the button has been pushed '
            'until RetroChadSql pauses.'))
        label.grid(sticky='w', pady=3, padx=3, columnspan=99)
        sep = ttk.Separator(frame, orient='horizontal')
        sep.grid(row=10, columnspan=9999, sticky='ew', pady=3)
        url = self._tk_url(frame, 'http://www.alltimersports.com')
        url.grid(row=20, column=0, sticky='sw', padx=3, pady=3)
        version = ttk.Label(frame, text='Version ' +
                            self._constants['version'])
        version.grid(row=20, column=10, sticky='se', padx=3, pady=3)
        frame.columnconfigure(10, weight=1)
        frame.rowconfigure(20, weight=1)
        nb.add(frame, text='Intro')

    def _ask_years(self, parent):
        # Make the frame to choose the years to process.
        frame = ttk.LabelFrame(parent)
        label = ttk.Label(frame, wraplength=self._wrap_length, text=(
            'Choose the years to process.  For example, 1921 1940-1942 1969.  '
            'The default shown is all the years* Retrosheet has as of '
            'December, 2013.  Also as of Fall 2013, only NL games are '
            'included in 1921.\n'
            '* 2013 is available, but is excluded by default because it has '
            'an incompatability with Chadwick. Chadwick Baseball Bureau '
            '(http://www.chadwick-bureau.com) maintains an alternate event '
            'file download area that may have a compatible version.'))
        label.grid(padx=3)
        self._vars['years'] = tk.StringVar(value=self._constants['years'])
        entry = ttk.Entry(frame, textvariable=self._vars['years'])
        entry.select_clear()
        entry.grid(row=10, sticky='ew', padx=3)
        self._finish_frame(frame)

    def _ask_tables(self, parent):
        # Make the frame for choosing the tables to deal with.
        frame = ttk.Frame(parent)
        label = ttk.Label(frame, text='Select the tables to create.')
        label.grid(padx=3)
        last_col = 0
        self._vars['tables'] = {}
        for table in ['events', 'subs', 'games']:
            self._vars['tables'][table] = tk.BooleanVar(value=True)
            button = ttk.Checkbutton(frame, text=table,
                                     variable=self._vars['tables'][table])
            button.grid(row=0, column=last_col + 10, padx=3)
            last_col += 10
        self._finish_frame(frame)

    def _ask_log(self, parent):
        # Make the frame deciding the logging level.
        frame = ttk.Frame(parent)
        log_levels = ['silent', 'normal', 'verbose', 'chatterbox']
        label = ttk.Label(frame, text='How much logging to see whie running:')
        label.grid(padx=3)
        self._vars['log'] = tk.IntVar(value=1)
        last_col = 0
        for i, text in enumerate(['silent', 'normal', 'verbose',
                                  'chatterbox']):
            button = ttk.Radiobutton(frame, text=text,
                                     variable=self._vars['log'], value=i)
            button.grid(row=0, column=last_col + 10, padx=3)
            last_col += 10
        self._finish_frame(frame)

    def _ask_tasks(self, parent, key, index, text):
        # Make a frame to pick the [first | last] step the program runs.
        frame = ttk.Frame(parent)
        self._vars[key] = tk.StringVar(value=self._tasks.keys()[index])
        label = ttk.Label(frame, wraplength=self._wrap_length, text=text)
        label.grid(sticky='w', columnspan=999, padx=3)
        last_col = -10
        for task_name in self._tasks.keys():
            button = ttk.Radiobutton(
                frame, text=self._tasks[task_name]['gerund'],
                variable=self._vars[key], value=task_name)
            button.grid(row=10, column=last_col + 10, padx=3)
            last_col += 10
        self._finish_frame(frame)

    def _ask_first_last(self, parent):
        # Call _ask_tasks twice to pick which steps of the program run.
        frame = ttk.Frame(parent)
        self._ask_tasks(frame, 'first', 0, (
            'If you already have the files that would be created by some step '
            'of RetroChadSql, you can start with the next step for each year. '
            'Start with...'))
        self._ask_tasks(frame, 'last', -1, (
            'You don\'t have to process each year all the way to uploading to '
            'the SQL database.  You may stop at any point after you start.  '
            'Stop after...'))
        self._finish_frame(frame, False)

    def _ask_rcs_dir(self, parent):
        # Make the frame to pick the program's home directory.
        frame = ttk.Frame(parent)
        label = ttk.Label(frame, text=(
            'Where should RcsChadSQL create any folders whose paths you '
            'don\'t specify?'))
        label.grid(columnspan=999, sticky='w', padx=3)
        frame.last_row = 0
        default = os.path.join(self._envir.user_dir, 'RetroChadSql')
        text = 'Base RetroChadSql Folder:'
        self._vars['rcs_dir'] = self._choose_var(frame, text, default, True)
        frame.columnconfigure(0, weight=1)
        self._finish_frame(frame, False)
        
    def _ask_general(self, nb):
        # Make the main control tab.
        frame = ttk.Frame(nb)
        self._ask_years(frame)
        self._ask_tables(frame)
        self._ask_log(frame)
        self._ask_first_last(frame)
        self._ask_rcs_dir(frame)
        nb.add(frame, text='General')
        return frame

    def _explain_chadwick(self, parent):
        # Make a frame to explain Chadwick and give urls to get it.
        frame = ttk.Frame(parent)
        label = ttk.Label(frame, wraplength=self._wrap_length, text=(
            'You will need Chadwick to process unzipped Retrosheet files into '
            'CSVs.  You will also need Chadwick to make the SQL definition'
            'files.\n\nFor Windows, Chadwick is available as a zip file that '
            'you can just unzip and leave in the folder you choose.  For '
            'other systems, the cource code is available for you to compile.'
            '\n\nThe Windows zip file is at'))
        label.grid(row=10, column=0, sticky='nw', padx=3)
        url = ('http://sourceforge.net/projects/chadwick/files/latest/'
               'download?source=files')
        url_label = self._tk_url(frame, url, wraplength=self._wrap_length)
        url_label.grid(row=20, column=0, sticky='nw', padx=3)
        label = ttk.Label(frame, wraplength=self._wrap_length,
                          text="\nThe source files can be found at")
        label.grid(row=30, column=0, sticky='nw', padx=3)
        url = 'http://sourceforge.net/projects/chadwick/files/'
        url_label = self._tk_url(frame, url, wraplength=self._wrap_length)
        url_label.grid(row=40, column=0, sticky='nw', padx=3)
        self._finish_frame(frame)

    def _standard_path(self, parent, home, var):
        # Show a default path with the last part editable.
        frame = ttk.Frame(parent)
        kwarg = {('textvariable' if isinstance(home, tk.StringVar) else
                  'text'): home}
        homeLabel = ttk.Label(frame, **kwarg)
        homeLabel.grid(padx=(3, 0))
        ttk.Label(frame, text=os.sep).grid(row=0, column=10)
        entry = ttk.Entry(frame, textvariable=var)
        entry.grid(row=0, column=20, sticky='ew', padx=(0, 3))
        frame.columnconfigure(20, weight=1)
        return frame

    def _show_prereq(self, parent, task):
        try:
            if not self._tasks[task]['prereq']:
                return
        except KeyError:  # task == 'Chadwick'
            return
        frame = ttk.Frame(parent)
        label = ttk.Label(frame, text=self._tasks[task]['prereq'],
                          wraplength=self._wrap_length)
        label.grid()
        frame.columnconfigure(0, weight=1)
        return frame

    def _toggle_frame(self, frame, state):
        for widget in frame.grid_slaves():
            if isinstance(widget, ttk.Frame):
                self._toggle_frame(widget, state)
            else:
                try:
                    widget.config(state=state)
                except TclError: pass

    def _toggle_custom(self, tab, custom):
        enablee, disablee = ((tab._custom_frame, tab._base_frame)
                             if custom else
                             (tab._base_frame, tab._custom_frame))
        def f():
            self._toggle_frame(enablee, 'normal')
            self._toggle_frame(disablee, 'disabled')
        return f

    def _ask_path(self, parent, task):
        # Make a frame to pick a path in a standard parent folder or ...
        # a custom path anywhere.
        
        frame = ttk.Frame(parent)
        
        task_info = (self._tasks[task] if task in self._tasks else
                     self._tasks.chad_info)
        file_description = task_info['file_description']
        default_name = file_description.split(' ')[0]
        if task == 'Chadwick':
            base_text = 'in user dir'
            home = self._envir.user_dir
            base_dir = home
            self._explain_chadwick(parent)
        else:
            base_text = 'in main folder'
            home = self._vars['rcs_dir']
            base_dir = home.get()
            
        path_label = ttk.Label(frame, text=
            'Where should {description} files {action}?'.format(
                description=file_description,
                action=('be found' if task == 'Chadwick' else 'go')))
        path_label.grid(sticky='w', padx=3, pady=(3, 0), columnspan=999)
        vars_ = {}
        vars_['custom'] = tk.BooleanVar(value=False)
        vars_['dir'] = tk.StringVar(value=default_name)
        init_path = os.path.join(base_dir, default_name)

        parent._base_frame = self._standard_path(frame, home, vars_['dir'])
        parent._base_frame.grid(row=10, column=10, sticky='ew')
        parent._custom_frame = ttk.Frame(frame)
        vars_['path'] = self._choose_var(
            parent._custom_frame, file_description + ' path', init_path, True,
            vars_['custom'])
        parent._custom_frame.columnconfigure(0, weight=1)
        parent._custom_frame.grid(row=20, column=10, pady=3, columnspan=99,
                                  sticky='ew')

        base_button = ttk.Radiobutton(
            frame, text=base_text, variable=vars_['custom'], value=False,
            command=self._toggle_custom(parent, False))
        base_button.grid(row=10, column=0, sticky='sw', padx=3)
        custom_button = ttk.Radiobutton(
            frame, text='custom location', variable = vars_['custom'],
            value=True, command=self._toggle_custom(parent, True))
        custom_button.grid(row=20, column=0, sticky='sw', padx=3, pady=(0, 3))
        self._toggle_frame(parent._custom_frame, 'disabled')

        prereq_frame = self._show_prereq(frame, task)
        if prereq_frame:
            sep = ttk.Separator(frame, orient='horizontal')
            sep.grid(columnspan=99, row=30, column=0, pady=3)
            prereq_frame.grid(row=40, column=0, pady=3, padx=3, sticky='nw',
                              columnspan=99)
            
        frame.columnconfigure(10, weight=1)

        try:
            self._vars[task].update(vars_)
        except KeyError:
            self._vars[task] = vars_
        self._finish_frame(frame)

    def _ask_keep(self, parent, task):
        # Make a frame to decide whether to delete files when done.
        frame=ttk.Frame(parent)
        label = ttk.Label(frame, text='Keep or Delete this folder when done?')
        label.grid(padx=3)
        var = tk.BooleanVar(value=(True if task == 'Chadwick' else False))
        keep_button = ttk.Radiobutton(frame, text='keep', variable=var,
                                      value=True)
        keep_button.grid(row=0, column=10, padx=3)
        delete_button = ttk.Radiobutton(frame, text='delete', variable=var,
                                        value=False)
        delete_button.grid(row=0, column=20, padx=3)
        self._vars[task]['keep'] = var
        label = ttk.Label(frame, wraplength=self._wrap_length, text=(
            'Be careful.  If "delete" is chosen, all files in the folder will '
            'be deleted.  (Only applies if this type of file is accessed or '
            'created for the steps you set RetroChadSql to run.)'))
        label.grid(row=10, sticky='w', padx=(3, 3), pady=3, columnspan=999)
        self._finish_frame(frame, False)

    def _ask_files(self, nb, task):
        # Make the tab to deal with files pertaining to <task>.
        frame=ttk.Frame(nb)
        if task == 'Define':
            self._vars['Define'] = {'db_name': self._ask_db_name(frame)}
        self._ask_path(frame, task)
        self._ask_keep(frame, task)
        frame.columnconfigure(0, weight=1)
        nb.add(frame, text=task.partition(' ')[0])
        return frame

    def _ask_db_name(self, parent):
        # Make the frame asking for the database name.
        frame = ttk.Frame(parent)
        label = ttk.Label(frame, wraplength=self._wrap_length, text=(
            'For simplest use, choose a database name that does not already '
            'exist on your server.  If you choose an existing database, '
            'RetroChadSql will add tables to the database.  If tables of '
            'those names are already in the database, RetroChadSql will '
            'attempt to insert data into those tables.  If the tables do not '
            'have the structure to allow that, RetroChadSql will stop and '
            'report the error.'))
        label.grid(padx=3, pady=(3, 0), sticky='w', columnspan=99)
        var = tk.StringVar(value='RetroChadSql')
        label = ttk.Label(frame, text='Name of database to use or create:')
        label.grid(row=10, column=0, padx=3, sticky='s')
        entry = ttk.Entry(frame, textvariable=var)
        entry.grid(row=10, column=10, padx=3, pady=(3, 0), sticky='ew')
        frame.columnconfigure(10, weight=1)
        self._finish_frame(frame)
        return var

    def _ask_client(self, parent):
        # Make a frame to pick the SQL command shell.
        frame = ttk.Frame(parent)
        # client_path is tuple(bool in-PATH_env, str path)
        client_path = self._envir.get_sql_client()
        label = ttk.Label(frame, wraplength=self._wrap_length, text=(
            ('RetroChadSql found what looks like a SQL command shell in your '
             'system path.  If it\'s not the right program, you can change '
             'it.')
                 if client_path[0] else
            ('RetroChadSql did not find a SQL command shell in your system '
             'path, but it did find a likely SQL command shell on your '
             'computer.  If it didn\'t find the right program, you can change '
             'it.')
                 if client_path[1] else
            ('RetroChadSql could not find a SQL command shell on your '
             'computer. If you want to write to a database, you will have to '
             'enter the path to the shell yourself.')))
        label.grid(columnspan=99, padx=3, pady=(0, 3), sticky='w')
        frame.last_row = 0
        var = self._choose_var(frame, 'SQL command shell',
                               client_path[1] or self._envir.user_dir, False)
        frame.columnconfigure(0, weight=1)
        self._finish_frame(frame)
        return var

    def _ask_user(self, parent):
        # Make a frame where the user may sepcify SQL user name, ...
        # password, hostname, and port.
        frame = ttk.Frame(parent)
        last_col = -10
        vars_ = {}
        for arg in ['User', 'Password', 'Host', 'Port']:
            text = arg
            label = ttk.Label(frame, text=text)
            vars_[arg] = tk.StringVar()
            entry=ttk.Entry(frame, width=12, textvariable=vars_[arg])
            label.grid(row=0, column=last_col + 10, padx=(3, 0))
            entry.grid(row=0, column=last_col + 20, padx=(0, 3))
            last_col += 20
        self._finish_frame(frame, False)
        return vars_

    def _ask_string(self, parent):
        # Make a frame where the user can specify a SQL connection string.
        frame = ttk.Frame(parent)
        label = ttk.Label(frame, text="SQL connection string (w/o file name)")
        label.grid(padx=3)
        var = tk.StringVar()
        entry=ttk.Entry(frame, textvariable=var)
        entry.grid(row=0, column=10, sticky='ew')
        frame.columnconfigure(10, weight=1)
        self._finish_frame(frame, False)
        return var

    def _ask_params(self, parent):
        # Make the frame for SQL connection configuration.
        frame = ttk.Frame(parent)
        label = ttk.Label(frame, wraplength=self._wrap_length, text=(
            'Your SQL setup may include a config file that automatically '
            'loads your password and other connection information.  IF SO, '
            'YOU DON\'T NEED TO DO ANYTHING HERE. If you have such a file but '
            'it doesn\'t load automatically, you can select its location.  If '
            'you don\'t have such a file at all, you can enter any or all of '
            'your user name, your password, the port and the hostname, as '
            'needed.  Alternatively, if you know the command line syntax to '
            'access your database server, you can eneter the argument names '
            'and values manually.  Do not include the file name of the shell.'
            ))
        label.grid(padx=3, columnspan=99, sticky='w')
        frame.last_row = 0
        vars_ = {}
        vars_['ini'] = self._choose_var(frame, 'SQL config file', '', False)
        ttk.Label(frame, text='Or').grid(row=frame.last_row + 10, column=0)
        frame.last_row += 10
        vars_.update(self._ask_user(frame))
        ttk.Label(frame, text='Or').grid(row=frame.last_row + 10, column=0)
        frame.last_row += 10
        vars_['string'] = self._ask_string(frame)
        frame.columnconfigure(0, weight=1)
        self._finish_frame(frame, False)
        return vars_

    def _ask_connect(self, nb):
        # Make the tab that deals with the SQL client connection.
        frame = ttk.Frame(nb)
        vars_ = {}
        vars_['shell'] = self._ask_client(frame)
        vars_.update(self._ask_params(frame))
        self._vars['Load'] = vars_
        frame.columnconfigure(0, weight=1)
        nb.add(frame, text='Database')
        return frame

    def _show_license(self, nb):
        # Make the license tab.
        frame = ttk.Frame(nb)
        ttk.Label(frame, text=self._constants['license']).grid(padx=3)
        nb.add(frame, text='License')
        return frame

    def _show_rcs_credit(self, parent):
        # Make the RetroChadSql credits frame on the credits tab.
        frame = ttk.Frame(parent)
        label = ttk.Label(frame, wraplength=self._wrap_length, text=(
            'RetroChadSql is provded by All Timer Sports. It is written by '
            'Dvd Avins with Julian LIghton.  See also the License tab.'))
        label.grid(pady=(3, 0), padx=3, sticky='w')
        frame.columnconfigure(0, weight=1)
        self._finish_frame(frame)

    def _show_retro_credit(self, parent):
        # Make the Retrosheet credits frame on the credits tab.
        frame = ttk.Frame(parent)
        label = ttk.Label(frame, wraplength=self._wrap_length, text=(
            'RetroChadSql uses the data provided by Retrosheet. The following '
            'is Retrosheet\'s requirement for redistribution of that data.'))
        label.grid(padx=3, sticky='w')
        label = ttk.Label(frame, wraplength=self._wrap_length, text=(
            'The information used here was obtained free of charge from and '
            'is copyrighted by Retrosheet.  Interested parties may contact '
            'Retrosheet at 20 Sunset Rd., Newark, DE 19711.'))
        label.grid(row=10, column=0, padx=3, sticky='w')
        frame.columnconfigure(0, weight=1)
        self._finish_frame(frame)

    def _show_chad_credit(self, parent):
        # Make the Chadwick credits frame on the credits tab.
        frame = ttk.Frame(parent)
        label = ttk.Label(frame, wraplength=self._wrap_length, text=(
            'RetroChadSql uses Chadwick to make CSV files from unzipped '
            'Retrosheet files.  It also consults the Chadwick help text when '
            'creating database definitions.'))
        label.grid(padx=3, sticky='w')
        label = ttk.Label(frame, wraplength=self._wrap_length, text=(
            'Chadwick is written, maintained, and Copyright (c) 2002-2013 by '
            'T. L. Turocy (ted.turocy@gmail.com) at Chadwick Baseball Bureau '
            '(http://www.chadwick-bureau.com).\nChadwick is licensed under '
            'the terms of the GNU General Public License.'))
        label.grid(row=10, column=0, padx=3, sticky='w')
        frame.columnconfigure(0, weight=1)
        self._finish_frame(frame, False)

    def _show_credits(self, nb):
        # Make the credits tab.
        frame = ttk.Frame(nb)
        self._show_rcs_credit(frame)
        self._show_retro_credit(frame)
        self._show_chad_credit(frame)
        frame.columnconfigure(0, weight=1)
        nb.add(frame, text='Credits')
        return frame

    def _require_input(self, value, key, msg, tab, dic=None):
        dic = (self._config if dic is None else dic)
        if value:
           dic[key] = value
        else:
            self._errors.insert(0, msg)
            self._show_tab = self._tabs[tab]
            
    def _find_tasks(self):
        # Determine and return which tasks will be performed.
        tasks = collections.OrderedDict()
        include = False
        for task in self._tasks.keys():
            if self._vals['first'] == task:  # When we get to <first>,...
                include = True  # ...start adding tasks.
            if include:
                tasks[task] = {'action': 'do'}
            if self._vals['last'] == task:  # After adding <last>,...
                break  # ...stop adding tasks.
        return tasks

    def _require_path(self, key):
        path_vals = self._vals[key]
        if key == 'Chadwick':
            dic = self._config['Chadwick'] = {}
        else:
            dic = self._config['tasks'].setdefault(key,
                                                  {'action': 'access files'})
        if path_vals['custom']:
            msg = 'No {path_type} path selected.'.format(path_type=key)
            path = os.path.join(path_vals['path'], '')
            self._require_input(path, 'path', msg, key, dic)
        else:
            if key == 'Chadwick':
                parent = self._envir.user_dir
            else:
                self._require_rcs_dir = True
                parent = self._vals['rcs_dir']                
            dic['path'] = (os.path.join(parent, path_vals['dir'], ''))

    def _required_paths(self, tasks):
        self._tasks.set_attr('needed_paths', [
            {'Download'},
            {'Download', 'Unzip'},
            {'Unzip', 'Assemble', 'Chadwick'},
            {'Unzip', 'Assemble', 'Define', 'Chadwick'},
            #maybe examine CSV instead of including 'Unzip' in last line.
            #{'Assemble', 'Define', 'Chadwick'}])
            {'Assemble', 'Define', 'Load'}])
        return reduce(set.union,
                      [self._tasks[task]['needed_paths'] for task in tasks])

    def _bad_years(self):
        msg = ("Bad years input" if self._vars['years'] else
               "No years given.")
        self._errors.insert(0, msg)
        self._show_tab = self._tabs['General']

    def _require_paths(self, tasks):
        self._require_rcs_dir = False
        path_set = self._required_paths(tasks)
        for task in path_set - {'Load'}:
            self._require_path(task)
            try:
                self._config['tasks'][task]['keep'] = self._vals[task]['keep']
            except KeyError:
                self._config['Chadwick']['keep'] = self._vals[task]['keep']
        if self._require_rcs_dir:
            msg = "No RetroChadSql folder for standard paths."
            path = os.path.join(self._vals['rcs_dir'], '')
            self._require_input(path, 'rcs_dir', msg, 'General')
        if 'Load' in path_set:
            msg = "No SQL client selected."
            self._require_input(self._vals['Load']['shell'], 'client_path',
                                msg, 'Load')
    
    def _parse_years(self):
        specs = self._vals['years'].split()
        if not specs:
            return self._bad_years()
        years = []
        for elt in specs:
            if '-' in elt:
                try:
                    new = range(int(elt[:4]), int(elt[-4:]) +1)
                except TypeError:
                    return self._bad_years()
                if new not in years:
                    years += new
            else:
                new = [int(elt)]
                if new not in years:
                    years += new
        self._config['years'] = map(str, years)

    def _test_connection(self, connect_string):
        test = connect_string + ' -e "SELECT 0;"'
        try:
            subprocess.check_call(test, shell=True)
        except subprocess.CalledProcessError:
            self._errors.insert(0, 'Can\'t access SQL client.')
            self._show_tab = self._tabs['Load']

    def _connect_string(self, load_params):
        client = self._config['client_path']
        connect_string = '"{client}"'.format(client=client)
        if load_params['ini']:
            form = ' --defatuls-extra-file="{path}"'
            connect_string += form.format(path=load_params['ini'])
        if load_params['string']:
            connect_string += ' ' + load_params['string']
        else:
            form = ' --{name}={value}'
            for param in ['User', 'Password', 'Host', 'Port']:
                if load_params[param]:
                    connect_string += form.format(
                        name=param.lower(), value=load_params[param])
        self._test_connection(connect_string)
        return connect_string

    def _set_config(self):
        self._errors = []
        self._show_tab = None
        self._vals = self._get_all(self._vars)
        self._config = {}
        
        tasks = self._find_tasks()
        self._require_input(tasks, 'tasks', "No tasks selected.", 'General')
        self._require_paths(tasks)
        if 'Define' in tasks:
            db_name = self._vals['Define']['db_name']
            msg = 'No database name given'
            dic = self._config['tasks']['Define']
            self._require_input(db_name, 'db_name', msg, 'Define', dic)
        
        tables = {table for table in self._vals['tables']
                  if self._vals['tables'][table]}  # {k if v}
        self._require_input(tables, 'tables', "No tables selected.",
                            'General')
        
        self._parse_years()

        if 'Load' in tasks:
            self._config['connect'] = self._connect_string(self._vals['Load'])
            
        self._config['log_level'] = self._vals['log']
    
    def _submit(self):
        # Handle the Go! button.
        self._set_config()
        if self._errors:
            message = '\n'.join(self._errors)
            showwarning('Incorrect Input', message)
            self._show_tab.master.select(self._show_tab)
        else:
            self._window.destroy()
            self._when_done(self._config)

    def _cancel(self):
        self._root.destroy()

    def _ok_cancel(self, parent):
        # Make and grid the frame with Submit and Cancel buttons.
        frame=ttk.Frame(parent)
        submit_button = ttk.Button(frame, text='Go!', command=self._submit)
        submit_button.grid(padx=3)
        cancel_button = ttk.Button(frame, text='Cancel', command=self._cancel)
        cancel_button.grid(row=0, column=10, padx=3)
        frame.grid(row=999)

    def setup(self):
        """ Ready the UI for the user."""
        self._tasks.set_attr('prereq', [
            None,
            ('To unzip the Retrosheet files, you will also need to specify '
             'the Download path where zipped files are to be found.'),
            ('To assemble the data into CSV files, you will also need to '
             'specify both the Unzip path where the Retrosheet files are to '
             'found AND the Chadwick path, where Chadwick is to be found.'),
            ('To define the tables, you will also need to specify both the '
             'Chadwick path where Chadwick is to be found AND the Assemble '
             'path where at least on CSV file (for the first year you include '
             'on the General tab) can be found.'),
            ('To load data into the database, you will also need to specify '
             'the Assemble path, where the data in CSV files can be found.')])
        self._vars = {}
        nb = ttk.Notebook(self._window)
        nb_tabs = {}
        nb_tabs['Intro'] = self._show_intro(nb)
        nb_tabs['General'] = self._ask_general(nb)
        for task_name in self._tasks.keys()[:-1]:
            nb_tabs[task_name] = self._ask_files(nb, task_name)
        nb_tabs['Load'] = self._ask_connect(nb)
        nb_tabs['Chadwick'] = self._ask_files(nb, 'Chadwick')
        nb_tabs['License'] = self._show_license(nb)
        nb_tabs['Credits'] = self._show_credits(nb)
        nb.enable_traversal()
        self._tabs = nb_tabs
        nb.grid()
        self._ok_cancel(self._window)


class FuncError(Exception):
    """Common class to handle exceptions thrown by task functions.

    Processor._step() calls task functions. If those functions throw an
    exception, it is caught and paseed here for handling.

    __init__ requires the original exception, the year that did not
    complete, and the gerund of the task whose function did not
    complete.

    year is that was being processed when the exception was thrown.

    notice() returns an explanaition of the original exception, suitable
    for reporting to the user.
    
    """

    def __init__(self, e, year, gerund):
        """e is an exception thrown by a task's function that is called
        by Processor._step().
        
        year is the year being processed when e was thrown occured.
        
        gerund is the gerund of the task whose function threw e.

        """
        self._kind = type(e).__name__
        self._year = year
        self._gerund = gerund
        self._text = self._error_text(e)

    def _error_text(self, exception):
        # Return information about specific Exception type instances.
        try:
            raise exception
        except SystemExit:
            raise
        except subprocess.CalledProcessError:
            return exception.output
        except urllib2.HTTPError:
            code = exception.code
            explanations = BHRH.responses[code]
            return ', '.join(code, explanations[0], explanations[1])
        except urllib2.URLError:
            return exception.reason
        except BadZipfile:
            return "The file may have been corrupted during downloading."
        except:
            return str(exception)

    def notice(self):
        """Return an explanaition of the original exception """
        form = ('{kind} error on {year} {gerund}:  {text}\n'
                'All files and directories and will be kept.')
        return form.format(kind=self._kind, year=self._year,
                           gerund=self._gerund, text=self._text)
                        

class Reporter(object):
    """Prettifies and prints if logging level is met.

    init takes an int for noisiness.
    report() does the printing, if ignorability <= init's noisiness.

    """
    """If this file is made into a package, this class should be
    made into its own file. Perhpas the class would be turned into
    module-level functions.

    """

    def __init__(self, parent, root, noisiness):
        """Outputs reports to a Tkinter ScrolledText widget.

        __init__ paramaters:
        noisiness determines who readily output will be reported.
        parent is the master widget of the ScrolledText.

        call report() to output top the ScrolledText.

        """
        root.deiconify()
        root.geometry('+80+3')
        self._root = root
        self._noisiness = noisiness
        self._log_box = ScrolledText(parent, wrap=tk.WORD)
        self._log_box.grid(padx=3, pady=3, sticky='news')
        
    def _pretty_map(self, d, indents):
        #Formats a dictionary. Mutually recursive with _prep_report().
        prepped = {self._prep_report(k, indents): self._prep_report(v, indents)
                       for k, v in d.items()}
        indent = ' ' * indents * 4
        tab_width = max(map(len, prepped.keys())) + 2
        justified = [indent + k.ljust(tab_width) + v
                         for k, v in prepped.items()]
        return '\n'.join(justified)

    def _prep_report(self, text, indents=-1):
        # Format anything.
        # Self-recursive and mutually recursive with _pretty_map().
        return (text if
                    isinstance(text, basestring) else
                (
                    ('' if
                         indents == -1 else
                     '...>\n') +
                 self._pretty_map(text, indents + 1)) if
                     isinstance(text, (collections.Mapping,
                                       collections.MutableMapping)) else
                ', '.join([self._prep_report(elt, indents) for elt in text]) if
                    isinstance(text, collections.Iterable) else
                unicode(text))

    def report(self, ignorability, *args):
        """Outputs prettified text, only if ignorability is low enough.

        If multiple arguments follow an allowably low ignorability,
        each will be processed and concatenated.

        """
        if ignorability > self._noisiness:
            return
        for arg in args:
            self._log_box.insert(tk.END, self._prep_report(arg))
        self._log_box.insert(tk.END, '\n')
        self._log_box.see(tk.END)
        self._root.update_idletasks()


class Processer(object):
    def __init__(self, root, envir, tasks, config):
        self._root = root
        self._envir = envir
        self._config = config
        self._tasks = tasks
        self._tasks.set_attr(
            'func',
            [self._download, self._unzip, self._assemble, self._define,
             self._load])
        self._schema_defined = False
        self._schema_loaded = False

        if config['log_level']:
            root.geometry('+80+3')
            self._reporter = Reporter(root, root, config['log_level'])
        else:
            root.withdraw()

    def process(self):    
        # Initialize the run-time and call the generator.
        
        self._tables = {name: Table(name, self._envir)
                        for name in self._config['tables']}
                        
        try:
            self._reporter.report(1, "Starting RetroChadSql")
            self._reporter.report(2, 'User input is:\n', self._config, '\n')
        except AttributeError: pass

        self._old_dirs = set()
        paths = {name: attributes['path']
                 for name, attributes in self._config['tasks'].items()
                 if name != 'Load'}
        try:
            paths['Chadwick'] = self._config['Chadwick']['path']
        except KeyError: pass
        Table.set_class_attributes(paths)
        for path in paths.values():
            self._old_dirs.add(self._envir.exist_path(path))
        for path in paths.values():
            if not os.path.exists(path):
                os.makedirs(path)

        run_tasks = [task for task in self._config['tasks']
                     if self._config['tasks'][task]['action'] == 'do']

        self._original_dir = os.getcwd()
        if set(run_tasks).intersection({'Assemble', 'Define'}):
            os.chdir(self._config['tasks']['Unzip']['path'])
            try:
                for table in self._tables.values():
                    # Test Chadwick while doing something useful.
                    table.parse_description()
            except subprocess.CalledProcessError as error:
                try:
                    self._reporter.report(0, "Error accessing Chadwick: ",
                                          error.output)
                except AttributeError:
                    self._reporter = Reporter(self._root, self._root, 0)
                    self._reporter.report(0, "Error accessing Chadwick: ",
                                          error.output)
                self._reporter.report(0, "Close this window to exit.")
                return

        self._root.stepper = self._step().next
        self._reset_caller(100)

    def _download(self, year):
        #Download a year's .zip file from Retrosheet.
        source_pattern = 'http://www.retrosheet.org/events/{year}eve.zip'
        zip_string = urllib2.urlopen(source_pattern.format(year=year)).read()
        write_dir = self._config['tasks']['Download']['path']
        file_name = os.path.join(write_dir, year + '.zip')
        with closing(open(file_name, 'wb')) as zip_file:
            zip_file.write(zip_string)


    def _unzip(self, year):
        # Unzip a year's Retrosheet data. All years share a directory.
        read_dir = self._config['tasks']['Download']['path']
        read_name = os.path.join(read_dir, year + '.zip')
        ZipFile(read_name).extractall(self._config['tasks']['Unzip']['path'])


    def _assemble(self, year):
        """Use Chadwick to make a year's CSV file for each table."""
        for table in self._tables.values():
            table.assemble_year(year)



    def _define_schema(self, db_name, sql_dir, year):
        file_name = os.path.join(sql_dir, 'schema.sql')
        with closing(open(file_name, 'w')) as schema:
            schema.write('CREATE DATABASE IF NOT EXISTS ' + db_name + ';\n')
            schema.write('USE ' + db_name + ';\n\n')
            for table in self._tables.values():
                # Supply a dummy year for Chadwick.
                table.define_schema(schema, year)
                #TODO: write here instead of passing schema.
            self._schema_defined = True
            

    def _define(self, year):
        db_name = '`' + self._config['tasks']['Define']['db_name'] + '`'
        sql_dir = self._config['tasks']['Define']['path']
        if not self._schema_defined:
            # Supply a dummy year for Chadwick.
            self._define_schema(db_name, sql_dir, year)
        file_path = os.path.join(sql_dir, year + '.sql')
        sql_statements = ['USE {db_name};'.format(db_name=db_name)]
        for table in self._tables.values():
            sql_statements.append(table.load_specs(year))
        with closing(open(file_path, 'w')) as sql_file:
            sql_file.write('\n\n'.join(sql_statements))

    def _load(self, year):
        sql_dir = self._config['tasks']['Define']['path']
        if not self._schema_loaded:
            self._sql_form = '{connect} < "{{sql_file}}"'.format(
                connect=self._config['connect'])
            schema_file = os.path.join(sql_dir, 'schema.sql')
            command = self._sql_form.format(sql_file=schema_file)
            subprocess.check_output(command, shell=True)
            self._schema_loaded = True
        load_file = os.path.join(sql_dir, year + '.sql')
        command = self._sql_form.format(sql_file=load_file)
        subprocess.check_output(command, shell=True)

    def _cleanup(self):
        os.chdir(self._original_dir)
        
        targets = set()
        for name, dic in self._config['tasks'].items():
            if name != 'Load' and not dic['keep']:
                targets.add(dic['path'])
        try:
            if not self._config['Chadwick']['keep']:
                targets.add(self._config['Chadwick']['path'])
        except KeyError: pass

        for target in targets:
            for file_name in os.listdir(target):
                file_path = os.path.join(target, file_name)
                try:
                    os.remove(file_path)
                except OSError: pass  # Probably a directory.

        for target in targets:
            dir_name = target
            while True:
                if dir_name in self._old_dirs:
                    break
                try:
                    os.rmdir(dir_name)  # Unless empty, throws OSError.
                except OSError:
                    break
                dir_name = os.path.dirname(dir_name)

    def _reset_caller(self, time=0):
        root = self._root
        root.caller = root.after(time, root.stepper)

    def _step(self):
        """A generator that allows Tk to update while running.

        Run each task's function for each year. Log progress. Allow Tk to
        update the UI between steps.

        Tk's updates are often behind real time and it appears unresponsive
        for periods of time. Users are warned of that.

        """
        do_tasks = [task for task in self._config['tasks'].keys()
                    if self._config['tasks'][task]['action'] == 'do']

        try:
            for year in self._config['years']:
                for task in do_tasks:
                    
                    func = self._tasks[task]['func']
                    gerund = self._tasks[task]['gerund']
                    try:
                        self._reporter.report(3, 'Starting ', year, ' ',
                                              gerund, '.')
                    except AttributeError: pass
                    self._reset_caller()
                    yield
                    
                    try:
                        func(year)
                    except Exception as e:
                        raise FuncError(e, year, gerund)
                    
                    try:
                        self._reporter.report(2, year, ' ', gerund,
                                              ' complete.')
                    except AttributeError: pass
                    self._reset_caller()
                    yield
                try:
                    self._reporter.report(1, year, ' complete.')
                except AttributeError: pass
                
        except FuncError as e:
            try:
                self._reporter.report(0, e.notice())
            except AttributeError:
                self._reporter = Reporter(self._root, self._root, 0)
                self._reporter.report(0, e.notice())
        else:
            try:
                self._reporter.report(2, "Starting cleanup.")
            except AttributeError: pass
            self._reset_caller()
            yield
            self._cleanup()

        # Either finish or tell user to.
        try:
            self._reporter.report(0, "Close this window to exit.")
        except AttributeError:
            self._root.destroy()  # Destroy tkinter.
        # Even though tkinter is destroyed, Python root variable exists.
        # And therefore root.caller may be yielded to.
        yield


class RetroChadSql(object):
    def __init__(self):
        self._envir = Environment()
        self._root = tk.Tk()
        self._root.title('RetroChadSql')
        self._tasks = Tasks([(name, {'name': name})
            for name in ['Download', 'Unzip', 'Assemble', 'Define', 'Load']])
        self._tasks.set_attr(
            'gerund',
            ['downloading', 'unzipping', 'assembling', 'defining', 'loading'])
        constants = {'version': VERSION,
                     'license': LICENSE,
                     'years': AVAILABLE_YEARS}
        self._input = Input(self._root, constants, self._envir, self._tasks,
                            self._process)

    def go(self):
        self._input.setup()
        self._root.mainloop()

    def _process(self, config):
        processer = Processer(self._root, self._envir, self._tasks, config)
        processer.process()


def main():
    rcs = RetroChadSql()
    rcs.go()

if __name__ == '__main__':
    main()

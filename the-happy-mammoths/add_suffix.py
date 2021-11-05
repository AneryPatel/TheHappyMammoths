import pandas as pd

def add_suffix_column(dataframe, table):
    suffix_list = []
    for i in dataframe[table]:

        if 'III' in i[-3:]:
            suffix_list.append('III')
        elif ('IV' in i[-2:] and len(i)>4):
            suffix_list.append('IV')
        elif ('I' in i[-1:] and i[-2] ==' '):
            suffix_list.append('I')
        elif('II' in i[-2:]):
            suffix_list.append('II')
        elif ('V' in i[-2:] and i[-2] == ' '):
            suffix_list.append('V')
        elif ('SR' in i[-2:] and i[-3] ==' '):
            suffix_list.append('SR')
        elif ('JR' in i[-2:] and i[-3] ==' '):
            suffix_list.append('JR')
        else:
            suffix_list.append('')

    dataframe['officer_suffix_name'] = suffix_list

    return(dataframe['officer_suffix_name'])


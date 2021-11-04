lis = []
for i in last_name:
    if 'III' in i[-3:] or ('IV' in i[-2:] and len(i)>4) or ('I' in i[-1:] and i[-2] ==' ') or ('II' in i[-2:]) or ('V' in i[-2:] and i[-2] == ' ') or ('SR' in i[-2:] and i[-3] ==' ') or ('JR' in i[-2:] and i[-3] ==' '):
        lis.extend([i.split(' ')])
    else:
        lis.append([i])

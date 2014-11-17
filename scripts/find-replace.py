import os
import sys
rootdir = sys.argv[1]

def inplace_change(filename, old_string, new_string):
    s=open(filename).read()
    if old_string in s:
        print 'Changing "{old_string}" to "{new_string}"'.format(**locals())
        s=s.replace(old_string, new_string)
        f=open(filename, 'w')
        f.write(s)
        f.flush()
        f.close()
    else:
        print 'No occurances of "{old_string}" found.'.format(**locals())






for root, dirs, files in os.walk(rootdir):
    for fname in files:
        inplace_change(fname,"/var/run","/var/run") 

#from scipy.stats import kurtosistest
#import numpy
from scipy.stats import ttest_ind
from raw_dat import RawDat
import os.path

files = [ '/dls/b21/data/2016/sw14620-1/processed/b21-59142.unsub/b21-59142_sample_0.dat', '/dls/b21/data/2016/sw14620-1/processed/b21-59142.unsub/b21-59142_sample_1.dat', '/dls/b21/data/2016/sw14620-1/processed/b21-59142.unsub/b21-59142_sample_2.dat', '/dls/b21/data/2016/sw14620-1/processed/b21-59142.unsub/b21-59142_sample_3.dat', '/dls/b21/data/2016/sw14620-1/processed/b21-59142.unsub/b21-59142_sample_4.dat', '/dls/b21/data/2016/sw14620-1/processed/b21-59142.unsub/b21-59142_sample_5.dat', '/dls/b21/data/2016/sw14620-1/processed/b21-59142.unsub/b21-59142_sample_6.dat', '/dls/b21/data/2016/sw14620-1/processed/b21-59142.unsub/b21-59142_sample_7.dat', '/dls/b21/data/2016/sw14620-1/processed/b21-59142.unsub/b21-59142_sample_8.dat', '/dls/b21/data/2016/sw14620-1/processed/b21-59142.unsub/b21-59142_sample_9.dat', '/dls/b21/data/2016/sw14620-1/processed/b21-59143.unsub/b21-59143_sample_0.dat', '/dls/b21/data/2016/sw14620-1/processed/b21-59143.unsub/b21-59143_sample_1.dat', '/dls/b21/data/2016/sw14620-1/processed/b21-59143.unsub/b21-59143_sample_2.dat', '/dls/b21/data/2016/sw14620-1/processed/b21-59143.unsub/b21-59143_sample_3.dat', '/dls/b21/data/2016/sw14620-1/processed/b21-59143.unsub/b21-59143_sample_4.dat', '/dls/b21/data/2016/sw14620-1/processed/b21-59143.unsub/b21-59143_sample_5.dat' ]

def BoxCar(myarray, window_size=5):
    if type(window_size) == type(1):
        if window_size % 2 == 0:
            print 'window_size needs to be an odd number'
            return False
        else:
            window_size = window_size
    else:
        print 'window size needs to be an integer'
        return False
    if not type(myarray) == type([]):
        print 'needs an array object'
        return False
    if len(myarray) < window_size+1:
        print 'test array needs to be larger than the window size'
        return False
        
        
    averaged = []
    lower_index = window_size/2
    upper_index = len(myarray)-lower_index
    index = lower_index + 1
    for item in myarray[lower_index:upper_index]:
        averaged.append(numpy.average(myarray[index-(lower_index+1):index+lower_index]))
        index += 1
                        
    return averaged

def TestSimilar(myarray):
    reference = 0
    for myfile in myarray:
        mydat = RawDat(myfile)
        if reference == 0:
            reference = mydat.ReturnColumn('I')
        else:
            target = mydat.ReturnColumn('I')
            if not len(target) == len(reference):
                print 'q range not the same, skipping file'
            ttest = ttest_ind(target, reference)
            if ttest[1] <= 0.98:
                return False
            else:
                return True

#print os.path.split(myfile)[-1]+','+str(ttest[0]_ind(difference))


TestSimilar(files)
    
